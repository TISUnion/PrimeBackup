import contextlib
import stat
import tarfile
import time
from io import BytesIO
from pathlib import Path
from typing import ContextManager, Optional, IO, Any

from typing_extensions import override

from prime_backup.action.export_backup_action_base import _ExportBackupActionBase
from prime_backup.compressors import Compressor
from prime_backup.constants import BACKUP_META_FILE_NAME
from prime_backup.db import schema
from prime_backup.types.export_failure import ExportFailures
from prime_backup.types.tar_format import TarFormat
from prime_backup.utils import blob_utils, platform_utils
from prime_backup.utils.bypass_io import BypassReader


class PeekReader:
	def __init__(self, file_obj: IO[bytes], peek_size: int):
		self.file_obj = file_obj
		self.peek_size = peek_size
		self.peek_buf: Optional[bytes] = None
		self.peek_buf_idx = 0

	def peek(self):
		if self.peek_buf is not None:
			raise RuntimeError('double peek')
		self.peek_buf = self.file_obj.read(self.peek_size)

	def read(self, n: int = -1) -> bytes:
		if self.peek_buf is None:
			raise RuntimeError('read before peek')

		if self.peek_buf_idx == len(self.peek_buf):
			return self.file_obj.read(n)

		if n == -1:
			data = self.peek_buf[self.peek_buf_idx:] + self.file_obj.read(n)
			self.peek_buf_idx = len(self.peek_buf)
			return data
		else:
			remaining = len(self.peek_buf) - self.peek_buf_idx
			if n <= remaining:
				data = self.peek_buf[self.peek_buf_idx:self.peek_buf_idx + n]
				self.peek_buf_idx += n
				return data
			else:
				data = self.peek_buf[self.peek_buf_idx:] + self.file_obj.read(n - remaining)
				self.peek_buf_idx = len(self.peek_buf)
				return data


class ExportBackupToTarAction(_ExportBackupActionBase):
	def __init__(self, backup_id: int, output_path: Path, tar_format: TarFormat, **kwargs):
		super().__init__(backup_id, output_path, **kwargs)
		self.tar_format = tar_format

	@override
	def is_interruptable(self) -> bool:
		return True

	@contextlib.contextmanager
	def __open_tar(self) -> ContextManager[tarfile.TarFile]:
		with open(self.output_path, 'wb') as f:
			compressor = Compressor.create(self.tar_format.value.compress_method)
			with compressor.compress_stream(f) as f_compressed:
				with tarfile.open(fileobj=f_compressed, mode=self.tar_format.value.mode_w) as tar:
					yield tar

	def __export_file(self, tar: tarfile.TarFile, file: schema.File):
		info = tarfile.TarInfo(name=file.path)
		info.mode = file.mode

		if file.uid is not None:
			info.uid = file.uid
			if (uid_name := platform_utils.uid_to_name(int(file.uid))) is not None:
				info.uname = uid_name
		if file.gid is not None:
			info.gid = file.gid
			if (gid_name := platform_utils.gid_to_name(int(file.gid))) is not None:
				info.gname = gid_name
		if file.mtime is not None:
			info.mtime = int(file.mtime / 1e9)
		if stat.S_ISREG(file.mode):
			if self.LOG_FILE_CREATION:
				self.logger.debug('add file {} to tarfile'.format(file.path))
			info.type = tarfile.REGTYPE
			info.size = file.blob_raw_size
			blob_path = blob_utils.get_blob_path(file.blob_hash)

			with Compressor.create(file.blob_compress).open_decompressed(blob_path) as stream:
				# Exception raised in TarFile.addfile might nuke the whole remaining tar file, which is bad
				# We read a few bytes from the stream, to *hopefully* trigger potential decompress exception in advanced,
				# make it fail before affecting the actual tar file
				peek_reader = PeekReader(stream, 32 * 1024)
				peek_reader.peek()

				if self.verify_blob:
					reader = BypassReader(peek_reader, calc_hash=True)
					tar.addfile(tarinfo=info, fileobj=reader)
				else:
					reader = None
					peek_reader: Any
					tar.addfile(tarinfo=info, fileobj=peek_reader)
			if reader is not None:
				# notes: the read len is always <= info.size
				self._verify_exported_blob(file, reader.get_read_len(), reader.get_hash())

		elif stat.S_ISDIR(file.mode):
			if self.LOG_FILE_CREATION:
				self.logger.debug('add dir {} to tarfile'.format(file.path))
			info.type = tarfile.DIRTYPE
			tar.addfile(tarinfo=info)
		elif stat.S_ISLNK(file.mode):
			if self.LOG_FILE_CREATION:
				self.logger.debug('add symlink {} to tarfile'.format(file.path))
			link_target = file.content.decode('utf8')
			info.type = tarfile.SYMTYPE
			info.linkname = link_target
			tar.addfile(tarinfo=info)
		else:
			self._on_unsupported_file_mode(file)

	@override
	def _export_backup(self, session, backup: schema.Backup) -> ExportFailures:
		failures = ExportFailures(self.fail_soft)
		if not self.output_path.name.endswith(self.tar_format.value.extension):
			raise ValueError('bad output file extension for file name {!r}, should be {!r} for tar format {}'.format(
				self.output_path.name, self.tar_format.value.extension, self.tar_format.name,
			))

		self.logger.info('Exporting backup {} to tarfile {}'.format(backup, self.output_path))
		self.output_path.parent.mkdir(parents=True, exist_ok=True)

		try:
			with self.__open_tar() as tar:
				for file in session.get_backup_files(backup):
					if self.is_interrupted.is_set():
						self.logger.info('Export to tarfile interrupted')
						raise self._ExportInterrupted()

					with failures.handling_exception(file):
						try:
							self.__export_file(tar, file)
						except Exception as e:
							self.logger.error('Export file {!r} to tar {} failed: {}'.format(file.path, self.output_path, e))
							raise

				if self.create_meta:
					meta_buf = self._create_meta_buf(backup)
					info = tarfile.TarInfo(name=BACKUP_META_FILE_NAME)
					info.mtime = int(time.time())
					info.size = len(meta_buf)
					tar.addfile(tarinfo=info, fileobj=BytesIO(meta_buf))
		except Exception as e:
			with contextlib.suppress(OSError):
				self.output_path.unlink(missing_ok=True)
			if not isinstance(e, self._ExportInterrupted):
				raise

		return failures
