import errno
import logging
from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from io import BytesIO
from pathlib import Path
from typing import IO, cast, List

from typing_extensions import Optional, override

from prime_backup import logger
from prime_backup.action.get_chunk_action import GetBlobChunksAction
from prime_backup.cli.fuse.utils import fuse_operation_wrapper, FuseErrnoReturnError
from prime_backup.compressors import Compressor, CompressMethod
from prime_backup.db.values import BlobStorageMethod
from prime_backup.types.blob_info import BlobInfo
from prime_backup.types.chunk_info import OffsetChunkInfo, ChunkInfo
from prime_backup.utils import blob_utils, chunk_utils


class _FileReader(ABC):
	class NoSequenceRead(IOError):
		pass

	@abstractmethod
	def read(self, size: int, offset: int) -> bytes:
		...

	@abstractmethod
	def close(self):
		...


class _SingleFileReader(_FileReader):
	def __init__(self, file_ctx: AbstractContextManager[IO[bytes]]):
		self.file_ctx = file_ctx
		self.file_obj = file_ctx.__enter__()
		self.rewindable = self.file_obj.seekable()
		self.offset = 0

	@override
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

	@override
	def close(self):
		self.file_ctx.__exit__(None, None, None)

	@classmethod
	def create_from_file(cls, file_path: Path, compress_method: CompressMethod) -> '_SingleFileReader':
		if compress_method == CompressMethod.plain:
			return _SingleFileReader(open(file_path, 'rb'))
		else:
			decompressed_stream = Compressor.create(compress_method).open_decompressed(file_path)
			return _SingleFileReader(cast(AbstractContextManager[IO[bytes]], decompressed_stream))

	@classmethod
	def create_from_blob(cls, blob: BlobInfo) -> '_SingleFileReader':
		return _SingleFileReader.create_from_file(blob_utils.get_blob_path(blob.hash), blob.compress)

	@classmethod
	def create_from_chunk(cls, chunk: ChunkInfo) -> '_SingleFileReader':
		return _SingleFileReader.create_from_file(chunk_utils.get_chunk_path(chunk.hash), chunk.compress)


class _MultiFileReader(_FileReader):
	current_index: int
	current_offset_lower: int
	current_offset_upper: int
	current_reader: _SingleFileReader

	def __init__(self, chunks: List[OffsetChunkInfo]):
		if len(chunks) == 0:
			raise ValueError()
		self.chunks = chunks
		self.__reopen_to_chunk(0)

	def __reopen_to_chunk(self, idx: int):
		self.close()

		offset_chunk = self.chunks[idx]
		self.current_index = idx
		self.current_offset_lower = offset_chunk.offset
		self.current_offset_upper = offset_chunk.offset + offset_chunk.chunk.raw_size
		self.current_reader = _SingleFileReader.create_from_chunk(offset_chunk.chunk)

	@override
	def read(self, size: int, offset: int) -> bytes:
		# TODO
		raise NotImplementedError()

	@override
	def close(self):
		if self.current_reader is not None:
			self.current_reader.close()


class PrimeBackupFuseFile:
	def __init__(self, *, blob: Optional[BlobInfo] = None, buf: Optional[bytes] = None):
		self.logger: logging.Logger = logger.get()
		self.reader: _FileReader
		self.blob: Optional[BlobInfo]
		if blob is not None:
			self.blob = blob
			self.reader = self.__create_reader_from_blob(blob)
		elif buf is not None:
			self.blob = None
			self.reader = _SingleFileReader(BytesIO(buf))
		else:
			raise ValueError()

	@classmethod
	def __create_reader_from_blob(cls, blob: BlobInfo) -> _FileReader:
		if blob.storage_method == BlobStorageMethod.direct:
			return cls.__create_reader_from_blob_direct(blob)
		elif blob.storage_method == BlobStorageMethod.chunked:
			return cls.__create_reader_from_blob_chunked(blob)
		else:
			raise FuseErrnoReturnError(errno.EIO)

	@classmethod
	def __create_reader_from_blob_direct(cls, blob: BlobInfo) -> _FileReader:
		return _SingleFileReader.create_from_blob(blob)

	@classmethod
	def __create_reader_from_blob_chunked(cls, blob: BlobInfo) -> _FileReader:
		blob_chunks = GetBlobChunksAction(blob.id).run()
		if len(blob_chunks) == 0:  # is it possible?
			return _SingleFileReader(BytesIO(b''))
		return _MultiFileReader(blob_chunks)

	@fuse_operation_wrapper()
	def read(self, length: int, offset: int) -> bytes:
		try:
			return self.reader.read(length, offset)
		except _FileReader.NoSequenceRead:
			self.logger.warning(f'Backward seeking is not supported by blob {self.blob}')
			raise FuseErrnoReturnError(errno.ENOTSUP)

	@fuse_operation_wrapper()
	def flush(self):
		pass

	@fuse_operation_wrapper()
	def release(self, flags):
		self.reader.close()
