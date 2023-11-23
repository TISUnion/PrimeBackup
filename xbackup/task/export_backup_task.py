import contextlib
import enum
import json
import os
import shutil
import stat
import sys
import tarfile
import time
from io import BytesIO
from pathlib import Path
from typing import ContextManager, NamedTuple, Any, Callable

from xbackup import utils
from xbackup.compressors import Compressor, CompressMethod
from xbackup.db import schema
from xbackup.db.access import DbAccess
from xbackup.task.task import Task


class DbStateError(Exception):
	pass


class _ExportFormatItem(NamedTuple):
	extension: str
	compress_method: CompressMethod


class ExportFormat(enum.Enum):
	direct = enum.auto()
	zip = enum.auto()
	tar = enum.auto()
	tar_gz = enum.auto()


class ExportBackupTask(Task):
	def __init__(self, backup_id: int, output_path: Path, export_format: ExportFormat):
		super().__init__()
		self.export_format = export_format
		self.backup_id = backup_id
		self.output_path = output_path

	def run(self):
		with DbAccess.open_session() as session:
			backup = session.get_backup(self.backup_id)
			if backup is None:
				raise KeyError('backup with id {} not found'.format(self.backup_id))

			export_method: Callable[[schema.Backup], Any] = {
				ExportFormat.direct: self.__export_directly,
				ExportFormat.zip: self.__export_to_zip,
				ExportFormat.tar: self.__export_to_tar,
				ExportFormat.tar_gz: self.__export_to_tar,
			}[self.export_format]
			export_method(backup)

		self.logger.info('exporting done')

	@contextlib.contextmanager
	def __open_tar(self) -> ContextManager[tarfile.TarFile]:
		with open(self.output_path, 'wb') as f:
			if self.export_format == ExportFormat.tar:
				compress_method = CompressMethod.plain
			elif self.export_format == ExportFormat.tar_gz:
				compress_method = CompressMethod.gzip
			else:
				raise ValueError('unsupported export format {} for tar export'.format(self.export_format))
			compressor = Compressor.create(compress_method)
			with compressor.compress_stream(f) as f_compressed:
				with tarfile.open(fileobj=f_compressed, mode='w') as tar:
					yield tar

	@classmethod
	def __create_meta_buf(cls, backup: schema.Backup) -> bytes:
		meta = {
			'_version': 1,
			'author': backup.author,
			'comment': backup.comment,
			'date': utils.timestamp_to_local_date(backup.timestamp),
		}
		return json.dumps(meta, indent=2, ensure_ascii=False).encode('utf8')

	def __export_directly(self, backup: schema.Backup):
		for file in backup.files:
			file: schema.File
			file_path = self.output_path / file.path

			if stat.S_ISREG(file.mode):
				self.logger.info('write file {}'.format(file.path))
				file_path.parent.mkdir(parents=True, exist_ok=True)
				blob: schema.Blob = file.blob
				blob_path = utils.get_blob_path(blob.hash)

				with Compressor.create(blob.compress).open_decompressed(blob_path) as f_in:
					with open(file_path, 'wb') as f_out:
						shutil.copyfileobj(f_in, f_out)

			elif stat.S_ISDIR(file.mode):
				file_path.mkdir(parents=True, exist_ok=True)
				self.logger.info('write dir {}'.format(file.path))
			else:
				# TODO: support other file types
				raise NotImplementedError('not supported yet')

			os.chmod(file_path, file.mode)
			os.utime(file_path, (time.time(), file.mtime_ns / 1e9))

			if sys.platform != 'win32':
				try:
					os.chown(file_path, file.uid, file.gid)
				except PermissionError:
					pass

	def __export_to_zip(self, backup: schema.Backup):
		# TODO
		raise NotImplementedError()

	def __export_to_tar(self, backup: schema.Backup):
		self.logger.info('exporting backup {} to tarfile {}'.format(backup, self.output_path))
		self.output_path.parent.mkdir(parents=True, exist_ok=True)

		with self.__open_tar() as tar:
			for file in backup.files:
				file: schema.File
				info = tarfile.TarInfo(name=file.path)
				info.mode = file.mode
				info.uid = file.uid
				info.gid = file.gid
				info.mtime = int(file.mtime_ns / 1e9)

				if stat.S_ISREG(file.mode):
					self.logger.info('add file {}'.format(file.path))
					info.type = tarfile.REGTYPE
					blob: schema.Blob = file.blob
					if blob is None:
						raise DbStateError('blob not found for file {}'.format(file))
					info.size = blob.size
					blob_path = utils.get_blob_path(blob.hash)

					with Compressor.create(blob.compress).open_decompressed(blob_path) as stream:
						tar.addfile(tarinfo=info, fileobj=stream)
				elif stat.S_ISDIR(file.mode):
					self.logger.info('add dir {}'.format(file.path))
					info.type = tarfile.DIRTYPE
					tar.addfile(tarinfo=info)
				else:
					# TODO: support other file types
					raise NotImplementedError('not supported yet')

			meta_buf = self.__create_meta_buf(backup)
			info = tarfile.TarInfo(name='.xbackup_meta.json')
			info.mtime = int(time.time())
			info.size = len(meta_buf)
			tar.addfile(tarinfo=info, fileobj=BytesIO(meta_buf))
