import contextlib
import dataclasses
import logging
import os
import queue
import shutil
import stat
import threading
import time
from pathlib import Path
from typing import Optional, List, Dict, Tuple

from typing_extensions import override, Unpack

from prime_backup import logger
from prime_backup.action.export_backup_action_base import _ExportBackupActionBase, ExportBackupActionCommonInitKwargs
from prime_backup.action.helpers.blob_exporter import BlobChunksGetter, ThreadSafeBlobChunksGetter
from prime_backup.action.helpers.progress_reporter import SizeProgressReporter
from prime_backup.action.helpers.restore_file_reuser import RestoreFileReuser, RestoreFileReuseRollbackError
from prime_backup.constants import constants
from prime_backup.db import schema
from prime_backup.db.session import DbSession
from prime_backup.types.export_failure import ExportFailures
from prime_backup.utils import file_utils, path_utils, collection_utils, pathspec_utils
from prime_backup.utils.thread_pool import FailFastBlockingThreadPool


def _i_am_root():
	# reference: tarfile.TarFile.chown
	return hasattr(os, 'geteuid') and os.geteuid() == 0


class _TrashBin:
	def __init__(self, trash_bin_path: Path):
		self.trash_bin_path = trash_bin_path
		self.trashes: List[Tuple[Path, Path]] = []  # (trash path, original path)
		self.absent_paths: List[Path] = []

	def add(self, src_path: Path, relative_path_in_bin: Path):
		dst_path = self.trash_bin_path / relative_path_in_bin
		dst_path.parent.mkdir(parents=True, exist_ok=True)
		shutil.move(src_path, dst_path)
		self.trashes.append((dst_path, src_path))

	def add_absent(self, original_path: Path):
		self.absent_paths.append(original_path)

	def restore(self):
		for original_path in self.absent_paths:
			file_utils.rm_rf(original_path, missing_ok=True)
		self.absent_paths.clear()
		for trash_path, original_path in self.trashes:
			file_utils.rm_rf(original_path, missing_ok=True)
			shutil.move(trash_path, original_path)
		self.trashes.clear()


@dataclasses.dataclass(frozen=True)
class _RetainedDirectoryAttrs:
	mode: int
	atime_ns: int
	mtime_ns: int

	@classmethod
	def read(cls, path: Path) -> '_RetainedDirectoryAttrs':
		st = path.lstat()
		return cls(st.st_mode, st.st_atime_ns, st.st_mtime_ns)

	def apply(self, path: Path):
		os.chmod(path, self.mode)
		os.utime(path, ns=(self.atime_ns, self.mtime_ns))


class _FileRetainer:
	def __init__(self, base_dir: Path, patterns: List[str], retain_dir: Path):
		self.logger: logging.Logger = logger.get()
		self.base_dir = base_dir
		self.patterns = pathspec_utils.compile_gitignore_spec(patterns)
		self.retain_dir = retain_dir
		self.__file_mappings: Dict[Path, Path] = {}  # temp path -> origin path
		self.__has_moved_away = False
		self.__moved_back_mappings: Dict[Path, Path] = {}
		self.__original_parent_attrs: Dict[Path, _RetainedDirectoryAttrs] = {}

	def move_away(self):
		if self.__has_moved_away:
			raise RuntimeError('use twice')
		self.__has_moved_away = True

		def on_error(e: Exception):
			raise e

		matched_paths: List[Tuple[str, Path]] = []
		excluded_temp_path: Optional[Path]
		try:
			excluded_temp_path = self.retain_dir.parent.relative_to(self.base_dir)
		except ValueError:
			excluded_temp_path = None
		for ent in self.patterns.match_tree_entries(self.base_dir, on_error=on_error, follow_links=False):
			ent_path = Path(ent.path)
			if excluded_temp_path is not None and path_utils.is_relative_to(ent_path, excluded_temp_path):
				continue
			matched_paths.append((ent_path.as_posix(), ent_path))

		for _, rel_path in sorted(matched_paths):  # parent first
			src_path = Path(self.base_dir) / rel_path
			dst_path = self.retain_dir / rel_path
			if os.path.lexists(src_path):  # check to see if the parent has already been moved
				self.logger.debug('Relocating retained file from {!r} to {!r}'.format(src_path.as_posix(), dst_path.as_posix()))
				if src_path.parent not in self.__original_parent_attrs:
					self.__original_parent_attrs[src_path.parent] = _RetainedDirectoryAttrs.read(src_path.parent)
				dst_path.parent.mkdir(parents=True, exist_ok=True)
				shutil.move(src_path, dst_path)
				self.__file_mappings[dst_path] = src_path
		self.logger.debug('Relocated {} retained files'.format(len(self.__file_mappings)))

	def move_back(self):
		for tmp_path, origin_path in list(self.__file_mappings.items()):
			shutil.move(tmp_path, origin_path)
			self.__file_mappings.pop(tmp_path)
			self.__moved_back_mappings[tmp_path] = origin_path
		if self.retain_dir.is_dir():
			shutil.rmtree(self.retain_dir)

	def rollback_move_back(self):
		for tmp_path, origin_path in reversed(list(self.__moved_back_mappings.items())):
			if os.path.lexists(origin_path):
				tmp_path.parent.mkdir(parents=True, exist_ok=True)
				shutil.move(origin_path, tmp_path)
			self.__moved_back_mappings.pop(tmp_path)
			self.__file_mappings[tmp_path] = origin_path

	def restore_original_parent_attrs(self):
		for path, attrs in sorted(self.__original_parent_attrs.items(), key=lambda item: len(item[0].parts), reverse=True):
			attrs.apply(path)
		self.__original_parent_attrs.clear()


class _ExportTempDirectory:
	def __init__(self, output_path: Path, retain_patterns: List[str]):
		from prime_backup.config.config import Config
		self.logger: logging.Logger = logger.get()
		config: Config = Config.get()

		# make temp_dir name
		self.__temp_dir_base_name = f'.{constants.PLUGIN_ID}.export_temp'
		temp_dir_name = f'{self.__temp_dir_base_name}_{os.getpid()}_{threading.current_thread().ident}'

		# decide temp_dir_path
		config.temp_path.mkdir(parents=True, exist_ok=True)
		if config.temp_path.stat().st_dev == output_path.stat().st_dev:
			self.__temp_dir_path = config.temp_path / temp_dir_name
		else:
			self.__temp_dir_path = output_path / temp_dir_name

		# init trash bin
		self.__trash_bin_path = self.__temp_dir_path / 'trash_bin'
		self.__trash_bin = _TrashBin(self.__trash_bin_path)

		# init retainer
		self.__retain_dir_path = self.__temp_dir_path / 'retained'
		self.__retainer: Optional[_FileRetainer] = None
		if len(retain_patterns) > 0:
			self.__retainer = _FileRetainer(output_path, retain_patterns, self.__retain_dir_path)

		self.logger.debug('Exporting temp directory {!r}'.format(self.__temp_dir_path))

	@property
	def trash_bin(self) -> _TrashBin:
		return self.__trash_bin

	@property
	def retainer(self) -> Optional[_FileRetainer]:
		return self.__retainer

	def prepare(self):
		try:
			# remove existing undeleted trash bins
			for f in self.__temp_dir_path.parent.iterdir():
				if f.name.startswith(self.__temp_dir_base_name):
					self.logger.warning('Removing existing undeleted temp dir {}'.format(f))
					file_utils.rm_rf(f)
		except OSError as e:
			self.logger.warning('Error when removing existing undeleted temp dirs: {}'.format(e))

		file_utils.rm_rf(self.__temp_dir_path, missing_ok=True)
		self.__trash_bin_path.mkdir(parents=True, exist_ok=True)
		if self.__retainer is not None:
			self.__retain_dir_path.mkdir(parents=True, exist_ok=True)

	def erase(self, *, make_writable: bool = False):
		if make_writable:
			self.__make_directories_writable(self.__temp_dir_path)
		shutil.rmtree(self.__temp_dir_path)

	@classmethod
	def __make_directories_writable(cls, path: Path):
		st = path.lstat()
		if not stat.S_ISDIR(st.st_mode):
			return
		os.chmod(path, st.st_mode | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
		with os.scandir(path) as entries:
			for entry in entries:
				if entry.is_dir(follow_symlinks=False):
					cls.__make_directories_writable(Path(entry.path))


class ExportBackupToDirectoryAction(_ExportBackupActionBase):
	@dataclasses.dataclass(frozen=True)
	class _ExportItem:
		file: schema.File
		path: Path  # path to export, related to self.output_path
		path_posix: str

	@dataclasses.dataclass(frozen=True)
	class _ExportedDirectory:
		file: schema.File
		path: Path

	def __init__(
			self, backup_id: int, output_path: Path, *,
			restore_mode: bool = False,
			child_to_export: Optional[Path] = None,
			recursively_export_child: bool = False,
			retain_patterns: Optional[List[str]] = None,
			**kwargs: Unpack[ExportBackupActionCommonInitKwargs],
	):
		"""
		:param restore_mode: recover what it was like -- delete all backup targets before export
		"""
		super().__init__(backup_id, **kwargs)
		self.output_path = output_path
		self.restore_mode = restore_mode
		self.child_to_export = child_to_export
		self.recursively_export_child = recursively_export_child
		self.retain_patterns: List[str] = retain_patterns or []

		if self.restore_mode and self.child_to_export is not None:
			raise ValueError('restore mode does not support exporting child')

	@classmethod
	def __set_attrs(cls, file: schema.File, file_path: Path):
		# reference: tarfile.TarFile.extractall, tarfile.TarFile._extract_member

		is_link = stat.S_ISLNK(file.mode)

		if _i_am_root() and file.uid is not None and file.gid is not None:
			u, g = int(file.uid), int(file.gid)
			if is_link and hasattr(os, 'lchown'):
				os.lchown(file_path, u, g)
			elif hasattr(os, 'chown'):
				os.chown(file_path, u, g)

		if not is_link:
			os.chmod(file_path, file.mode)

		if file.mtime is not None:
			times_ns = (time.time_ns(), file.mtime_unix_ns)  # (atime, mtime)
			if is_link:
				if os.utime in os.supports_follow_symlinks:
					os.utime(file_path, ns=times_ns, follow_symlinks=False)
			else:
				os.utime(file_path, ns=times_ns)

	def __prepare_for_export(self, item: _ExportItem, trash_bin: _TrashBin):
		file_path = self.output_path / item.path
		if os.path.lexists(file_path):
			trash_bin.add(file_path, item.path)
		file_path.parent.mkdir(parents=True, exist_ok=True)

	def __export_file(
			self, blob_chunks_getter: BlobChunksGetter, item: _ExportItem,
			exported_directories: 'queue.Queue[ExportBackupToDirectoryAction._ExportedDirectory]',
			reuser: Optional[RestoreFileReuser],
	):
		file = item.file
		file_path = self.output_path / item.path

		if stat.S_ISREG(file.mode):
			if reuser is not None and reuser.try_reuse(file, item.path):
				return
			if self.LOG_FILE_CREATION:
				self.logger.debug('write file {}'.format(file.path))
			self._create_blob_exporter(blob_chunks_getter, file).export_to_fs(file_path)

		elif stat.S_ISDIR(file.mode):
			if self.LOG_FILE_CREATION:
				self.logger.debug('write dir {}'.format(file.path))
			file_path.mkdir(parents=True, exist_ok=True)
			exported_directories.put(self._ExportedDirectory(file, file_path))

		elif stat.S_ISLNK(file.mode):
			if not file.content:
				raise AssertionError('symlink file {} has no content'.format(file))
			link_target = file.content.decode('utf8')
			os.symlink(link_target, file_path)
			if self.LOG_FILE_CREATION:
				self.logger.debug('write symbolic link {} -> {}'.format(file_path, link_target))
		else:
			self._on_unsupported_file_mode(file)

		if not stat.S_ISDIR(file.mode):
			self.__set_attrs(file, file_path)

	def __export_items(
			self, blob_chunks_getter: BlobChunksGetter, export_items: List[_ExportItem],
			failures: ExportFailures, reuser: Optional[RestoreFileReuser],
	) -> 'queue.Queue[ExportBackupToDirectoryAction._ExportedDirectory]':
		directories: 'queue.Queue[ExportBackupToDirectoryAction._ExportedDirectory]' = queue.Queue()
		progress = SizeProgressReporter(
			'Backup file export',
			total_count=len(export_items),
			total_size=sum(item.file.blob_raw_size or 0 for item in export_items),
		)

		def export_worker(item: ExportBackupToDirectoryAction._ExportItem):
			try:
				self.__export_file(blob_chunks_getter, item, directories, reuser)
			except RestoreFileReuseRollbackError:
				raise
			except Exception as e:
				with failures.handling_exception(item.file):
					self.logger.error('Export file {!r} to path {} failed: {}'.format(item.file.path, item.path, e))
					raise
			progress.on_one_file_done(item.file)

		with contextlib.ExitStack() as es:
			pool: Optional[FailFastBlockingThreadPool] = None
			if self.config.get_effective_concurrency() > 1:
				pool = es.enter_context(FailFastBlockingThreadPool('export'))
			for item in export_items:
				if pool is not None:
					pool.submit(export_worker, item)
				else:
					export_worker(item)

		return directories

	def __set_directory_attrs(
			self,
			directories: 'queue.Queue[ExportBackupToDirectoryAction._ExportedDirectory]',
			failures: ExportFailures,
	):
		def get_file_path(directory: ExportBackupToDirectoryAction._ExportedDirectory) -> str:
			return directory.file.path

		# Child directories first, after all child mutations have completed.
		for directory in sorted(
				collection_utils.drain_queue(directories),
				key=get_file_path,
				reverse=True,
		):
			with failures.handling_exception(directory.file):
				self.__set_attrs(directory.file, directory.path)

	@override
	def _export_backup(self, session: DbSession, backup: schema.Backup) -> ExportFailures:
		failures = ExportFailures(self.fail_soft)

		# 1. collect export item

		def add_export_item(file_: schema.File, export_path: Path):
			for t in backup.targets:
				if path_utils.is_relative_to(Path(file_.path), t):
					export_items.append(self._ExportItem(file_, export_path, export_path.as_posix()))
					return
			self.logger.warning('Found out-of-backup-target file, ignored. file.path: {!r}, backup.targets: {}'.format(file_.path, backup.targets))

		export_items: List[ExportBackupToDirectoryAction._ExportItem] = []
		if self.child_to_export is None:
			self.logger.info('Exporting {} to directory {}'.format(backup, self.output_path))
			for file in session.get_backup_files(backup):
				add_export_item(file, Path(file.path))
		else:
			self.logger.info('Exporting child {!r} in {} to directory {}, recursively = {}'.format(self.child_to_export.as_posix(), backup, self.output_path, self.recursively_export_child))
			for file in session.get_backup_files(backup):
				try:
					rel_path = Path(file.path).relative_to(self.child_to_export)
				except ValueError:
					continue
				if rel_path != Path('.') and not self.recursively_export_child:
					continue
				add_export_item(file, Path(self.child_to_export.name) / rel_path)

		# 2. do the export

		self.output_path.mkdir(parents=True, exist_ok=True)
		reuse_unchanged_files = self.restore_mode and self.config.restore.reuse_unchanged_files
		export_temp_dir = _ExportTempDirectory(self.output_path, self.retain_patterns)
		export_temp_dir.prepare()
		if export_temp_dir.retainer is not None:
			export_temp_dir.retainer.move_away()
		reuser: Optional[RestoreFileReuser] = None
		rolled_back = False
		try:
			if self.restore_mode:
				# in restore mode, recover what it was like
				# if the backup does not have the target, don't keep the target
				for target in backup.targets:
					target_path = self.output_path / target
					if os.path.lexists(target_path):
						export_temp_dir.trash_bin.add(target_path, Path(target))
					elif reuse_unchanged_files:
						export_temp_dir.trash_bin.add_absent(target_path)

			# parent dir first, so the parent will be added to trash-bin first
			def get_export_path(item: ExportBackupToDirectoryAction._ExportItem) -> str:
				return item.path_posix

			export_items.sort(key=get_export_path)
			for item in export_items:
				with failures.handling_exception(item.file):
					self.__prepare_for_export(item, export_temp_dir.trash_bin)

			ts_bcg = ThreadSafeBlobChunksGetter(session)
			if reuse_unchanged_files:
				reuser = RestoreFileReuser(
					self.output_path,
					export_temp_dir.trash_bin.trash_bin_path,
					apply_backup_attrs_func=self.__set_attrs,
				)
			directories = self.__export_items(ts_bcg, export_items, failures, reuser)

			# restore retained files before setting directory attrs
			if export_temp_dir.retainer is not None:
				export_temp_dir.retainer.move_back()

			self.__set_directory_attrs(directories, failures)
			if reuser is not None:
				reuser.commit()

		except Exception:
			self.logger.warning('Error occurs during export to directory, applying rollback')
			rolled_back = True
			try:
				if export_temp_dir.retainer is not None:
					export_temp_dir.retainer.rollback_move_back()
			finally:
				try:
					if reuser is not None:
						reuser.rollback()
				finally:
					export_temp_dir.trash_bin.restore()
			raise
		finally:
			if export_temp_dir.retainer is not None:
				export_temp_dir.retainer.move_back()
				if rolled_back:
					export_temp_dir.retainer.restore_original_parent_attrs()
			export_temp_dir.erase(make_writable=reuse_unchanged_files)

		return failures
