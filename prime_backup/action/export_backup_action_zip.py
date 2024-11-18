import contextlib
import os
import shutil
import stat
import time
import zipfile

from typing_extensions import override

from prime_backup.action.export_backup_action_base import _ExportBackupActionBase
from prime_backup.compressors import Compressor
from prime_backup.constants import BACKUP_META_FILE_NAME
from prime_backup.db import schema
from prime_backup.types.export_failure import ExportFailures
from prime_backup.utils import blob_utils
from prime_backup.utils.bypass_io import BypassReader


class ExportBackupToZipAction(_ExportBackupActionBase):
	@override
	def is_interruptable(self) -> bool:
		return True

	def __export_file(self, zipf: zipfile.ZipFile, file: schema.File):
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
			if self.LOG_FILE_CREATION:
				self.logger.debug('add file {} to zipfile'.format(file.path))
			info.file_size = file.blob_raw_size
			blob_path = blob_utils.get_blob_path(file.blob_hash)

			with Compressor.create(file.blob_compress).open_decompressed(blob_path) as stream:
				with zipf.open(info, 'w') as zip_item:
					if self.verify_blob:
						reader = BypassReader(stream, calc_hash=True)
						shutil.copyfileobj(reader, zip_item)
					else:
						reader = None
						shutil.copyfileobj(stream, zip_item)
			if reader is not None:
				self._verify_exported_blob(file, reader.get_read_len(), reader.get_hash())

		elif stat.S_ISDIR(file.mode):
			if self.LOG_FILE_CREATION:
				self.logger.debug('add dir {} to zipfile'.format(file.path))
			info.external_attr |= 0x10
			zipf.writestr(info, b'')
		elif stat.S_ISLNK(file.mode):
			if self.LOG_FILE_CREATION:
				self.logger.debug('add symlink {} to zipfile'.format(file.path))
			with zipf.open(info, 'w') as zip_item:
				zip_item.write(file.content)
		else:
			self._on_unsupported_file_mode(file)

	@override
	def _export_backup(self, session, backup: schema.Backup) -> ExportFailures:
		failures = ExportFailures(self.fail_soft)
		self.logger.info('Exporting backup {} to zipfile {}'.format(backup, self.output_path))
		self.output_path.parent.mkdir(parents=True, exist_ok=True)

		try:
			with zipfile.ZipFile(self.output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
				for file in session.get_backup_files(backup):
					if self.is_interrupted.is_set():
						self.logger.info('Export to zipfile interrupted')
						raise self._ExportInterrupted()

					with failures.handling_exception(file):
						try:
							self.__export_file(zipf, file)
						except Exception as e:
							self.logger.error('Export file {!r} to zip {} failed: {}'.format(file.path, self.output_path, e))
							raise

				if self.create_meta:
					meta_buf = self._create_meta_buf(backup)
					info = zipfile.ZipInfo(BACKUP_META_FILE_NAME, time.localtime()[0:6])
					info.compress_type = zipf.compression
					info.file_size = len(meta_buf)
					with zipf.open(info, 'w') as f:
						f.write(meta_buf)

		except Exception as e:
			with contextlib.suppress(OSError):
				self.output_path.unlink(missing_ok=True)
			if not isinstance(e, self._ExportInterrupted):
				raise

		return failures
