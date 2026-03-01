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
		self.file_seekable = self.file_obj.seekable()
		self.offset = 0

	@override
	def read(self, size: int, offset: int) -> bytes:
		if offset != self.offset:
			if self.file_seekable:
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
	def __init__(self, chunks: List[OffsetChunkInfo]):
		if len(chunks) == 0:
			raise ValueError()
		self.chunks = sorted(chunks)
		self.total_size = self.chunks[-1].offset + self.chunks[-1].size
		self.current_index: int = 0
		self.current_reader: Optional[_SingleFileReader] = None
		self.__reopen_to_chunk(0)

	@property
	def current_chunk(self) -> OffsetChunkInfo:
		return self.chunks[self.current_index]

	@property
	def current_offset_lower(self) -> int:
		return self.current_chunk.offset

	@property
	def current_offset_upper(self) -> int:
		return self.current_chunk.offset + self.current_chunk.size

	def __reopen_to_chunk(self, idx: int):
		self.close()

		self.current_index = idx
		self.current_reader = _SingleFileReader.create_from_chunk(self.chunks[idx].chunk)

	def __is_offset_in_index(self, offset: int, index: int) -> bool:
		if index < 0 or index >= len(self.chunks):
			return False
		chunk = self.chunks[index]
		return chunk.offset <= offset < chunk.offset + chunk.size

	def __find_chunk_index(self, offset: int) -> int:
		left, right = 0, len(self.chunks) - 1

		while left <= right:
			mid = (left + right) // 2
			mid_chunk = self.chunks[mid]
			chunk_offset = mid_chunk.offset
			chunk_end = chunk_offset + mid_chunk.size

			if offset < chunk_offset:
				right = mid - 1
			elif offset >= chunk_end:
				left = mid + 1
			else:
				return mid

		raise ValueError(f'Offset {offset} is out of range of all chunks')

	def __read_once(self, offset: int, size: int) -> bytes:
		if not self.__is_offset_in_index(offset, self.current_index):
			if self.__is_offset_in_index(offset, self.current_index + 1):
				target_idx = self.current_index + 1
			else:
				target_idx = self.__find_chunk_index(offset)
			self.__reopen_to_chunk(target_idx)

		relative_offset = offset - self.current_offset_lower
		read_size = min(size, self.current_offset_upper - offset)

		assert self.current_reader is not None
		return self.current_reader.read(read_size, relative_offset)

	@override
	def read(self, size: int, offset: int) -> bytes:
		if offset >= self.total_size:
			return b''

		buf_list: List[bytes] = []
		while True:
			buf = self.__read_once(offset, size)
			buf_list.append(buf)
			size -= len(buf)
			if size <= 0 or self.current_index >= len(self.chunks) - 1:
				break
			offset += len(buf)

		return b''.join(buf_list)

	@override
	def close(self):
		if self.current_reader is not None:
			self.current_reader.close()
			self.current_reader = None


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
