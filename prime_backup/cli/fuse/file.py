import errno
import logging
from contextlib import AbstractContextManager
from io import BytesIO
from typing import IO, cast

from typing_extensions import Optional

from prime_backup import logger
from prime_backup.cli.fuse.utils import fuse_operation_wrapper, FuseErrnoReturnError
from prime_backup.compressors import Compressor, CompressMethod
from prime_backup.types.blob_info import BlobInfo
from prime_backup.utils import blob_utils


class _Reader:
	class NoSequenceRead(IOError):
		pass

	def __init__(self, file_ctx: AbstractContextManager[IO[bytes]], rewindable: bool):
		self.file_ctx = file_ctx
		self.rewindable = rewindable
		self.file_obj = file_ctx.__enter__()
		self.offset = 0

	def read(self, size: int, offset: int) -> bytes:
		if offset != self.offset:
			can_seek = self.rewindable or self.offset < offset
			if can_seek:
				self.file_obj.seek(offset)
				self.offset = offset
			else:
				raise self.NoSequenceRead()

		buf = self.file_obj.read(size)
		self.offset += len(buf)
		return buf

	def close(self):
		self.file_ctx.__exit__(None, None, None)


class PrimeBackupFuseFile:
	def __init__(self, *, blob: Optional[BlobInfo] = None, buf: Optional[bytes] = None):
		self.logger: logging.Logger = logger.get()
		if blob is not None:
			self.blob = blob
			self.reader = self.__create_reader_from_blob(blob)
		elif buf is not None:
			self.blob = None
			self.reader = _Reader(BytesIO(buf), rewindable=True)
		else:
			raise ValueError()

	@classmethod
	def __create_reader_from_blob(cls, blob: BlobInfo) -> _Reader:
		blob_path = blob_utils.get_blob_path(blob.hash)
		compressor = Compressor.create(blob.compress)
		if compressor.get_method() == CompressMethod.plain:
			return _Reader(open(blob_path, 'rb'), rewindable=True)
		else:
			# XXX: allow rewind for compress methods that support it?
			decompressed_stream = compressor.open_decompressed(blob_path)
			return _Reader(cast(AbstractContextManager[IO[bytes]], decompressed_stream), rewindable=False)

	@fuse_operation_wrapper()
	def read(self, length: int, offset: int) -> bytes:
		try:
			return self.reader.read(length, offset)
		except _Reader.NoSequenceRead:
			self.logger.warning(f'Backward seeking is not supported by blob {self.blob}')
			raise FuseErrnoReturnError(errno.ENOTSUP)

	@fuse_operation_wrapper()
	def flush(self):
		pass

	@fuse_operation_wrapper()
	def release(self, flags):
		self.reader.close()
