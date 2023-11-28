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
from typing import ContextManager

from prime_backup.action import Action
from prime_backup.compressors import Compressor, CompressMethod
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.types.tar_format import TarFormat
from prime_backup.utils import file_utils, blob_utils, misc_utils


class ExportBackupActions:
	@classmethod
	def to_dir(cls, backup_id: int, output_path: Path, delete_existing: bool) -> 'ExportBackupActionBase':
		return ExportBackupToDirectoryAction(backup_id, output_path, delete_existing)

	@classmethod
	def to_tar(cls, backup_id: int, output_path: Path, tar_format: TarFormat) -> 'ExportBackupActionBase':
		return ExportBackupToTarAction(backup_id, output_path, tar_format)

	@classmethod
	def to_zip(cls, backup_id: int, output_path: Path) -> 'ExportBackupActionBase':
		return ExportBackupToZipAction(backup_id, output_path)


class ExportBackupActionBase(Action, ABC):
	def __init__(self, backup_id: int, output_path: Path):
		super().__init__()
		self.backup_id = misc_utils.ensure_type(backup_id, int)
		self.output_path = output_path

	def run(self):
		with DbAccess.open_session() as session:
			backup = session.get_backup(self.backup_id)
			self._export_backup(session, backup)

		self.logger.info('exporting done')

	@abstractmethod
	def _export_backup(self, session: DbSession, backup: schema.Backup):
		...

	@classmethod
	def _create_meta_buf(cls, backup: schema.Backup) -> bytes:
		meta = {
			'_version': 1,
			'author': backup.author,
			'comment': backup.comment,
			'timestamp': backup.timestamp,
			'targets': backup.targets,
			'hidden': backup.hidden,
		}
		return json.dumps(meta, indent=2, ensure_ascii=False).encode('utf8')


def _i_am_root():
	# reference: tarfile.TarFile.chown
	return hasattr(os, 'geteuid') and os.geteuid() == 0


class ExportBackupToDirectoryAction(ExportBackupActionBase):
	def __init__(self, backup_id: int, output_path: Path, delete_existing: bool):
		super().__init__(backup_id, output_path)
		self.delete_existing = delete_existing

	def __set_attrs(self, file: schema.File):
		file_path = self.output_path / file.path

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

	def _export_backup(self, session, backup: schema.Backup):
		self.logger.info('Exporting backup {} to directory {}'.format(backup, self.output_path))
		self.output_path.mkdir(parents=True, exist_ok=True)

		# clean up existing
		if self.delete_existing:
			for target in backup.targets:
				target_path = self.output_path / target
				if target_path.is_dir():
					shutil.rmtree(target_path)
				else:
					target_path.unlink(missing_ok=True)

		directories = []
		file: schema.File
		for file in backup.files:
			file_path = self.output_path / file.path

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
				directories.append(file)

			elif stat.S_ISLNK(file.mode):
				link_target = file.content.decode('utf8')
				os.symlink(link_target, file_path)
				self.logger.debug('write symbolic link {} -> {}'.format(file_path, link_target))
			else:
				# TODO: support other file types
				raise NotImplementedError('not supported yet')

			if not stat.S_ISDIR(file.mode):
				self.__set_attrs(file)

		# child dir first
		# reference: tarfile.TarFile.extractall
		for dir_file in sorted(directories, key=lambda d: d.path, reverse=True):
			self.__set_attrs(dir_file)


class ExportBackupToTarAction(ExportBackupActionBase):
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

	def _export_backup(self, session, backup: schema.Backup):
		if not self.output_path.name.endswith(self.tar_format.value.extension):
			raise ValueError('bad output file extension for file name {!r}, should be {!r} for tar format {}'.format(
				self.output_path.name, self.tar_format.value.extension, self.tar_format.name,
			))

		self.logger.info('Exporting backup {} to tarfile {}'.format(backup, self.output_path))
		self.output_path.parent.mkdir(parents=True, exist_ok=True)

		with self.__open_tar() as tar:
			file: schema.File

			for file in backup.files:
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
					info.size = file.blob_size
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
					raise NotImplementedError('not supported yet')

			meta_buf = self._create_meta_buf(backup)
			info = tarfile.TarInfo(name='.prime_backup.meta.json')
			info.mtime = int(time.time())
			info.size = len(meta_buf)
			tar.addfile(tarinfo=info, fileobj=BytesIO(meta_buf))


class ExportBackupToZipAction(ExportBackupActionBase):
	def _export_backup(self, session, backup: schema.Backup):
		self.logger.info('Exporting backup {} to zipfile {}'.format(backup, self.output_path))
		self.output_path.parent.mkdir(parents=True, exist_ok=True)

		with zipfile.ZipFile(self.output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
			file: schema.File
			for file in backup.files:
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
					info.file_size = file.blob_size
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
					info.external_attr |= 0xA1ED0000
					with zipf.open(info, 'w') as zip_item:
						zip_item.write(file.content)
				else:
					raise NotImplementedError('not supported yet')

			meta_buf = self._create_meta_buf(backup)
			info = zipfile.ZipInfo('.prime_backup.meta.json', time.localtime()[0:6])
			info.compress_type = zipf.compression
			info.file_size = len(meta_buf)
			with zipf.open(info, 'w') as f:
				f.write(meta_buf)
