import contextlib
import json
import os
import shutil
import stat
import tarfile
import time
import zipfile
from abc import abstractmethod, ABC
from io import BytesIO
from pathlib import Path
from typing import ContextManager, Optional, List, Tuple, NamedTuple, Union

from prime_backup.action import Action
from prime_backup.compressors import Compressor, CompressMethod
from prime_backup.constants import BACKUP_META_FILE_NAME
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.types.backup_meta import BackupMeta
from prime_backup.types.file_info import FileInfo
from prime_backup.types.tar_format import TarFormat
from prime_backup.utils import file_utils, blob_utils, misc_utils


class ExportFailure(NamedTuple):
	file: FileInfo
	error: Exception


class ExportFailures:
	def __init__(self, fail_soft: bool):
		self.__fail_soft = fail_soft
		self.failures: List[ExportFailure] = []

	def add_or_raise(self, file: Union[FileInfo, schema.File], error: Exception):
		if self.__fail_soft:
			if isinstance(file, schema.File):
				file = FileInfo.of(file)
			self.failures.append(ExportFailure(file, error))
		else:
			raise error

	def __len__(self) -> int:
		return len(self.failures)


class _ExportBackupActionBase(Action[ExportFailures], ABC):
	def __init__(self, backup_id: int, output_path: Path, *, fail_soft: bool = False):
		super().__init__()
		self.backup_id = misc_utils.ensure_type(backup_id, int)
		self.output_path = output_path
		self.fail_soft = fail_soft

	def run(self) -> ExportFailures:
		with DbAccess.open_session() as session:
			backup = session.get_backup(self.backup_id)
			failures = self._export_backup(session, backup)

		if len(failures) > 0:
			self.logger.info('Export done with {} failures'.format(len(failures)))
		else:
			self.logger.info('Export done')
		return failures

	@abstractmethod
	def _export_backup(self, session: DbSession, backup: schema.Backup) -> ExportFailures:
		...

	@classmethod
	def _create_meta_buf(cls, backup: schema.Backup) -> bytes:
		meta = BackupMeta.from_backup(backup)
		return json.dumps(meta.to_dict(), indent=2, ensure_ascii=False).encode('utf8')

	def _on_unsupported_file_mode(self, file: schema.File):
		raise NotImplementedError('file at {!r} with mode={} ({} or {}) is not supported yet'.format(file.path, file.mode, hex(file.mode), oct(file.mode)))


def _i_am_root():
	# reference: tarfile.TarFile.chown
	return hasattr(os, 'geteuid') and os.geteuid() == 0


class ExportBackupToDirectoryAction(_ExportBackupActionBase):
	def __init__(
			self, backup_id: int, output_path: Path, delete_existing: bool, *,
			child_to_export: Optional[Path] = None, recursively_export_child: bool = False,
	):
		super().__init__(backup_id, output_path)
		self.delete_existing = delete_existing
		self.child_to_export = child_to_export
		self.recursively_export_child = recursively_export_child

	@classmethod
	def __set_attrs(cls, file: schema.File, file_path: Path):
		# reference: tarfile.TarFile.extractall, tarfile.TarFile._extract_member

		if _i_am_root() and file.uid is not None and file.gid is not None:
			u, g = int(file.uid), int(file.gid)
			if stat.S_ISLNK(file.mode) and hasattr(os, 'lchown'):
				os.lchown(file_path, u, g)
			else:
				os.chown(file_path, u, g)

		if not stat.S_ISLNK(file.mode):
			os.chmod(file_path, file.mode)
			if file.atime_ns is not None and file.mtime_ns is not None:
				os.utime(file_path, (file.atime_ns / 1e9, file.mtime_ns / 1e9))

	def _export_backup(self, session, backup: schema.Backup) -> ExportFailures:
		failures = ExportFailures(self.fail_soft)
		if self.child_to_export is None:
			self.logger.info('Exporting {} to directory {}'.format(backup, self.output_path))
		else:
			self.logger.info('Exporting child {!r} in {} to directory {}, recursively = {}'.format(self.child_to_export.as_posix(), backup, self.output_path, self.recursively_export_child))
		self.output_path.mkdir(parents=True, exist_ok=True)

		# clean up existing
		if self.delete_existing:
			for target in backup.targets:
				target_path = self.output_path / target
				if target_path.is_dir():
					shutil.rmtree(target_path)
				else:
					target_path.unlink(missing_ok=True)

		directories: List[Tuple[schema.File, Path]] = []
		file: schema.File
		for file in backup.files:
			if self.child_to_export is not None:
				try:
					rel_path = Path(file.path).relative_to(self.child_to_export)
				except ValueError:
					continue
				if rel_path != Path('.') and not self.recursively_export_child:
					continue
				file_path = self.output_path / self.child_to_export.name / rel_path
				if rel_path == Path('.'):
					self.logger.info('Exporting child {!r} to {!r}'.format(file.path, file_path.as_posix()))
			else:
				file_path = self.output_path / file.path

			try:
				if stat.S_ISREG(file.mode):
					self.logger.debug('write file {}'.format(file.path))
					file_path.parent.mkdir(parents=True, exist_ok=True)
					blob_path = blob_utils.get_blob_path(file.blob_hash)
					compressor = Compressor.create(file.blob_compress)
					if compressor.get_method() == CompressMethod.plain:
						file_utils.copy_file_fast(blob_path, file_path)
					else:
						with compressor.open_decompressed(blob_path) as f_in:
							with open(file_path, 'wb') as f_out:
								shutil.copyfileobj(f_in, f_out)

				elif stat.S_ISDIR(file.mode):
					file_path.mkdir(parents=True, exist_ok=True)
					self.logger.debug('write dir {}'.format(file.path))
					directories.append((file, file_path))

				elif stat.S_ISLNK(file.mode):
					link_target = file.content.decode('utf8')
					os.symlink(link_target, file_path)
					self.logger.debug('write symbolic link {} -> {}'.format(file_path, link_target))
				else:
					self._on_unsupported_file_mode(file)

				if not stat.S_ISDIR(file.mode):
					self.__set_attrs(file, file_path)

			except Exception as e:
				failures.add_or_raise(file, e)

		# child dir first
		# reference: tarfile.TarFile.extractall
		for dir_file, dir_file_path in sorted(directories, key=lambda d: d[0].path, reverse=True):
			try:
				self.__set_attrs(dir_file, dir_file_path)
			except Exception as e:
				failures.add_or_raise(dir_file, e)

		return failures


class ExportBackupToTarAction(_ExportBackupActionBase):
	def __init__(self, backup_id: int, output_path: Path, tar_format: TarFormat):
		super().__init__(backup_id, output_path)
		self.tar_format = tar_format

	@contextlib.contextmanager
	def __open_tar(self) -> ContextManager[tarfile.TarFile]:
		with open(self.output_path, 'wb') as f:
			compressor = Compressor.create(self.tar_format.value.compress_method)
			with compressor.compress_stream(f) as f_compressed:
				with tarfile.open(fileobj=f_compressed, mode=self.tar_format.value.mode_w) as tar:
					yield tar

	def _export_backup(self, session, backup: schema.Backup) -> ExportFailures:
		failures = ExportFailures(self.fail_soft)
		if not self.output_path.name.endswith(self.tar_format.value.extension):
			raise ValueError('bad output file extension for file name {!r}, should be {!r} for tar format {}'.format(
				self.output_path.name, self.tar_format.value.extension, self.tar_format.name,
			))

		self.logger.info('Exporting backup {} to tarfile {}'.format(backup, self.output_path))
		self.output_path.parent.mkdir(parents=True, exist_ok=True)

		with self.__open_tar() as tar:
			file: schema.File

			for file in backup.files:
				try:
					info = tarfile.TarInfo(name=file.path)
					info.mode = file.mode

					if file.uid is not None:
						info.uid = file.uid
					if file.gid is not None:
						info.gid = file.gid
					if file.mtime_ns is not None:
						info.mtime = int(file.mtime_ns / 1e9)
					if stat.S_ISREG(file.mode):
						self.logger.debug('add file {} to tarfile'.format(file.path))
						info.type = tarfile.REGTYPE
						info.size = file.blob_raw_size
						blob_path = blob_utils.get_blob_path(file.blob_hash)

						with Compressor.create(file.blob_compress).open_decompressed(blob_path) as stream:
							tar.addfile(tarinfo=info, fileobj=stream)
					elif stat.S_ISDIR(file.mode):
						self.logger.debug('add dir {} to tarfile'.format(file.path))
						info.type = tarfile.DIRTYPE
						tar.addfile(tarinfo=info)
					elif stat.S_ISLNK(file.mode):
						self.logger.debug('add symlink {} to tarfile'.format(file.path))
						link_target = file.content.decode('utf8')
						info.type = tarfile.SYMTYPE
						info.linkname = link_target
						tar.addfile(tarinfo=info)
					else:
						self._on_unsupported_file_mode(file)

				except Exception as e:
					failures.add_or_raise(file, e)

			meta_buf = self._create_meta_buf(backup)
			info = tarfile.TarInfo(name=BACKUP_META_FILE_NAME)
			info.mtime = int(time.time())
			info.size = len(meta_buf)
			tar.addfile(tarinfo=info, fileobj=BytesIO(meta_buf))

		return failures


class ExportBackupToZipAction(_ExportBackupActionBase):
	def _export_backup(self, session, backup: schema.Backup) -> ExportFailures:
		failures = ExportFailures(self.fail_soft)
		self.logger.info('Exporting backup {} to zipfile {}'.format(backup, self.output_path))
		self.output_path.parent.mkdir(parents=True, exist_ok=True)

		with zipfile.ZipFile(self.output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
			file: schema.File
			for file in backup.files:
				try:
					# reference: zipf.writestr -> zipfile.ZipInfo.from_file
					if file.mtime_ns is not None:
						date_time = time.localtime(file.mtime_ns / 1e9)
					else:
						date_time = time.localtime()
					arc_name = file.path
					while len(arc_name) > 0 and arc_name[0] in (os.sep, os.altsep):
						arc_name = arc_name[1:]
					if stat.S_ISDIR(file.mode) and not arc_name.endswith('/'):
						arc_name += '/'

					info = zipfile.ZipInfo(arc_name, date_time[0:6])
					info.external_attr = (file.mode & 0xFFFF) << 16
					info.compress_type = zipf.compression

					if stat.S_ISREG(file.mode):
						self.logger.debug('add file {} to zipfile'.format(file.path))
						info.file_size = file.blob_raw_size
						blob_path = blob_utils.get_blob_path(file.blob_hash)

						with Compressor.create(file.blob_compress).open_decompressed(blob_path) as stream:
							with zipf.open(info, 'w') as zip_item:
								shutil.copyfileobj(stream, zip_item)

					elif stat.S_ISDIR(file.mode):
						self.logger.debug('add dir {} to zipfile'.format(file.path))
						info.external_attr |= 0x10
						zipf.writestr(info, b'')
					elif stat.S_ISLNK(file.mode):
						self.logger.debug('add symlink {} to zipfile'.format(file.path))
						with zipf.open(info, 'w') as zip_item:
							zip_item.write(file.content)
					else:
						self._on_unsupported_file_mode(file)

				except Exception as e:
					failures.add_or_raise(file, e)

			meta_buf = self._create_meta_buf(backup)
			info = zipfile.ZipInfo(BACKUP_META_FILE_NAME, time.localtime()[0:6])
			info.compress_type = zipf.compression
			info.file_size = len(meta_buf)
			with zipf.open(info, 'w') as f:
				f.write(meta_buf)

		return failures
