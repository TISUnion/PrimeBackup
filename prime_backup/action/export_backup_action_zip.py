import contextlib
import os
import stat
import time
import zipfile
from pathlib import Path
from typing import Union, BinaryIO, Generator

from typing_extensions import override, Unpack

from prime_backup.action.export_backup_action_base import _ExportBackupActionBase, ExportBackupActionCommonInitKwargs
from prime_backup.action.helpers.blob_exporter import BlobChunksGetter, ThreadSafeBlobChunksGetter
from prime_backup.constants.constants import BACKUP_META_FILE_NAME
from prime_backup.db import schema
from prime_backup.types.export_failure import ExportFailures
from prime_backup.utils import file_utils
from prime_backup.utils.io_types import SupportsReadBytes


class ExportBackupToZipAction(_ExportBackupActionBase):
	def __init__(self, backup_id: int, output_dest: Union[Path, BinaryIO], **kwargs: Unpack[ExportBackupActionCommonInitKwargs]):
		super().__init__(backup_id, **kwargs)
		self.output_dest = output_dest

	@override
	def is_interruptable(self) -> bool:
		return True

	@contextlib.contextmanager
	def __open_zipf(self) -> Generator[zipfile.ZipFile, None, None]:
		with zipfile.ZipFile(self.output_dest, 'w', zipfile.ZIP_DEFLATED) as zipf:
			yield zipf

	def __export_file(self, blob_chunks_getter: BlobChunksGetter, zipf: zipfile.ZipFile, file: schema.File):
		# reference: zipf.writestr -> zipfile.ZipInfo.from_file
		if file.mtime is not None:
			date_time = time.localtime(file.mtime_unix_sec)
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
			if self.LOG_FILE_CREATION:
				self.logger.debug('add file {} to zipfile'.format(file.path))
			if file.blob_raw_size is None:
				raise AssertionError('file {!r} with ISREG mode has no blob_raw_size'.format(file))
			info.file_size = file.blob_raw_size

			def reader_csm(reader: SupportsReadBytes):
				with zipf.open(info, 'w') as zip_item:
					file_utils.copy_file_obj_fast(reader, zip_item, estimate_read_size=file.blob_raw_size)

			self._create_blob_exporter(blob_chunks_getter, file).export_as_reader(reader_csm)
		elif stat.S_ISDIR(file.mode):
			if self.LOG_FILE_CREATION:
				self.logger.debug('add dir {} to zipfile'.format(file.path))
			info.external_attr |= 0x10
			zipf.writestr(info, b'')
		elif stat.S_ISLNK(file.mode):
			if self.LOG_FILE_CREATION:
				self.logger.debug('add symlink {} to zipfile'.format(file.path))
			with zipf.open(info, 'w') as zip_item:
				zip_item.write(file.content or b'')
		else:
			self._on_unsupported_file_mode(file)

	@override
	def _export_backup(self, session, backup: schema.Backup) -> ExportFailures:
		failures = ExportFailures(self.fail_soft)

		if isinstance(self.output_dest, Path):
			self.logger.info('Exporting backup {} to zipfile {}'.format(backup, self.output_dest))
			self.output_dest.parent.mkdir(parents=True, exist_ok=True)
		else:
			self.logger.info('Exporting backup {} to given BinaryIO object'.format(backup))

		ts_bcg = ThreadSafeBlobChunksGetter(session)
		try:
			with self.__open_zipf() as zipf:
				for file in session.get_backup_files(backup):
					if self.is_interrupted.is_set():
						self.logger.info('Export to zipfile interrupted')
						raise self._ExportInterrupted()

					with failures.handling_exception(file):
						try:
							self.__export_file(ts_bcg, zipf, file)
						except Exception as e:
							output_dest_str = str(self.output_dest) if isinstance(self.output_dest, Path) else str(type(self.output_dest))
							self.logger.error('Export file {!r} to zip {} failed: {}'.format(file.path, output_dest_str, e))
							raise

				if self.create_meta:
					meta_buf = self._create_meta_buf(backup)
					info = zipfile.ZipInfo(BACKUP_META_FILE_NAME, time.localtime()[0:6])
					info.compress_type = zipf.compression
					info.file_size = len(meta_buf)
					with zipf.open(info, 'w') as f:
						f.write(meta_buf)

		except Exception as e:
			if isinstance(self.output_dest, Path):
				with contextlib.suppress(OSError):
					self.output_dest.unlink(missing_ok=True)
			if not isinstance(e, self._ExportInterrupted):
				raise

		return failures
