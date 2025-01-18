import dataclasses
import os
import queue
import shutil
import stat
import threading
import time
from pathlib import Path
from typing import Optional, List, Tuple

from typing_extensions import override

from prime_backup import constants
from prime_backup.action.export_backup_action_base import _ExportBackupActionBase
from prime_backup.compressors import Compressor, CompressMethod
from prime_backup.db import schema
from prime_backup.db.session import DbSession
from prime_backup.types.export_failure import ExportFailures
from prime_backup.utils import file_utils, blob_utils, hash_utils, path_utils, collection_utils
from prime_backup.utils.bypass_io import BypassReader
from prime_backup.utils.thread_pool import FailFastBlockingThreadPool


def _i_am_root():
	# reference: tarfile.TarFile.chown
	return hasattr(os, 'geteuid') and os.geteuid() == 0


class _TrashBin:
	def __init__(self, trash_bin_path: Path):
		file_utils.rm_rf(trash_bin_path, missing_ok=True)
		trash_bin_path.mkdir(parents=True, exist_ok=True)

		self.trash_bin_path = trash_bin_path
		self.trashes: List[Tuple[Path, Path]] = []  # (trash path, original path)

	def add(self, src_path: Path, relpath_in_bin: Path):
		dst_path = self.trash_bin_path / relpath_in_bin
		dst_path.parent.mkdir(parents=True, exist_ok=True)
		shutil.move(src_path, dst_path)
		self.trashes.append((dst_path, src_path))

	def erase(self):
		shutil.rmtree(self.trash_bin_path)

	def restore(self):
		for trash_path, original_path in self.trashes:
			file_utils.rm_rf(original_path, missing_ok=True)
			shutil.move(trash_path, original_path)

		self.trashes.clear()


class ExportBackupToDirectoryAction(_ExportBackupActionBase):
	@dataclasses.dataclass(frozen=True)
	class _ExportItem:
		file: schema.File
		path: Path  # path to export, related to self.output_path
		path_posix: str

	def __init__(
			self, backup_id: int, output_path: Path, *,
			restore_mode: bool = False,
			child_to_export: Optional[Path] = None,
			recursively_export_child: bool = False,
			**kwargs,
	):
		"""
		:param restore_mode: recover what it was like -- delete all backup targets before export
		"""
		super().__init__(backup_id, output_path, **kwargs)
		self.restore_mode = restore_mode
		self.child_to_export = child_to_export
		self.recursively_export_child = recursively_export_child

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
			else:
				os.chown(file_path, u, g)

		if not is_link:
			os.chmod(file_path, file.mode)

		if file.mtime is not None:
			times = (time.time(), file.mtime / 1e9)
			if is_link:
				if os.utime in os.supports_follow_symlinks:
					os.utime(file_path, times, follow_symlinks=False)
			else:
				os.utime(file_path, times)

	def __prepare_for_export(self, item: _ExportItem, trash_bin: _TrashBin):
		file_path = self.output_path / item.path
		if os.path.lexists(file_path):
			trash_bin.add(file_path, item.path)
		file_path.parent.mkdir(parents=True, exist_ok=True)

	def __export_file(self, item: _ExportItem, exported_directories: 'queue.Queue[Tuple[schema.File, Path]]'):
		file = item.file
		file_path = self.output_path / item.path

		if stat.S_ISREG(file.mode):
			if self.LOG_FILE_CREATION:
				self.logger.debug('write file {}'.format(file.path))
			blob_path = blob_utils.get_blob_path(file.blob_hash)
			compressor = Compressor.create(file.blob_compress)
			if compressor.get_method() == CompressMethod.plain:
				file_utils.copy_file_fast(blob_path, file_path)
				if self.verify_blob:
					sah = hash_utils.calc_file_size_and_hash(file_path)
					self._verify_exported_blob(file, sah.size, sah.hash)
			else:
				with compressor.open_decompressed(blob_path) as f_in:
					with open(file_path, 'wb') as f_out:
						if self.verify_blob:
							reader = BypassReader(f_in, calc_hash=True)
							shutil.copyfileobj(reader, f_out)
						else:
							reader = None
							shutil.copyfileobj(f_in, f_out)
				if reader is not None:
					self._verify_exported_blob(file, reader.get_read_len(), reader.get_hash())

		elif stat.S_ISDIR(file.mode):
			if self.LOG_FILE_CREATION:
				self.logger.debug('write dir {}'.format(file.path))
			file_path.mkdir(parents=True, exist_ok=True)
			exported_directories.put((file, file_path))

		elif stat.S_ISLNK(file.mode):
			link_target = file.content.decode('utf8')
			os.symlink(link_target, file_path)
			if self.LOG_FILE_CREATION:
				self.logger.debug('write symbolic link {} -> {}'.format(file_path, link_target))
		else:
			self._on_unsupported_file_mode(file)

		if not stat.S_ISDIR(file.mode):
			self.__set_attrs(file, file_path)

	@override
	def _export_backup(self, session: DbSession, backup: schema.Backup) -> ExportFailures:
		failures = ExportFailures(self.fail_soft)

		# 1. collect export item

		def add_export_item(file_: schema.File, export_path: Path):
			for t in backup.targets:
				if path_utils.is_relative_to(Path(file_.path), t):
					export_items.append(self._ExportItem(file_, export_path, export_path.as_posix()))
					return
			self.logger.warning('Found out-of-backup-target file, ignored. file.path: {!r}, backup.targets: {}'.format(file, backup.targets))

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
		self.config.temp_path.mkdir(parents=True, exist_ok=True)
		trash_bin_name_base = f'.{constants.PLUGIN_ID}.export_trashes'
		trash_bin_dir_name = f'{trash_bin_name_base}_{os.getpid()}_{threading.current_thread().ident}'
		trash_bin_path = self.config.temp_path / trash_bin_dir_name
		if self.config.temp_path.stat().st_dev != self.output_path.stat().st_dev:
			trash_bin_path = self.output_path / trash_bin_dir_name
		try:
			# remove existing undeleted trash bins
			for f in trash_bin_path.parent.iterdir():
				if f.name.startswith(trash_bin_name_base):
					self.logger.warning('Removing existing undeleted trash bin {}'.format(f))
					file_utils.rm_rf(f)
		except OSError as e:
			self.logger.warning('Error when removing existing undeleted trash bins: {}'.format(e))

		trash_bin = _TrashBin(trash_bin_path)
		try:
			if self.restore_mode:
				# in restore mode, recover what it was like
				# if the backup does not have the target, don't keep the target
				for target in backup.targets:
					target_path = self.output_path / target
					if os.path.lexists(target_path):
						trash_bin.add(target_path, Path(target))

			# parent dir first, so the parent will be added to trash-bin first
			export_items.sort(key=lambda ei: ei.path_posix)
			for item in export_items:
				with failures.handling_exception(item.file):
					self.__prepare_for_export(item, trash_bin)

			directories: 'queue.Queue[Tuple[schema.File, Path]]' = queue.Queue()
			with FailFastBlockingThreadPool('export') as pool:
				def export_worker(item_: ExportBackupToDirectoryAction._ExportItem):
					with failures.handling_exception(item_.file):
						try:
							self.__export_file(item_, directories)
						except Exception as e_:
							self.logger.error('Export file {!r} to path {} failed: {}'.format(item_.file.path, item_.path, e_))
							raise

				for item in export_items:
					pool.submit(export_worker, item)

			# child dir first
			# reference: tarfile.TarFile.extractall
			for dir_file, dir_file_path in sorted(
					collection_utils.drain_queue(directories),
					key=lambda d: d[0].path,
					reverse=True,
			):
				with failures.handling_exception(dir_file):
					self.__set_attrs(dir_file, dir_file_path)

		except Exception:
			self.logger.warning('Error occurs during export to directory, applying rollback')
			trash_bin.restore()
			raise
		finally:
			trash_bin.erase()

		return failures
