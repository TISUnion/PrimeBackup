import contextlib
import dataclasses
import functools
import shutil
import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Callable, Any, List, Generator

from typing_extensions import override

from prime_backup.compressors import CompressMethod
from prime_backup.compressors import Compressor
from prime_backup.db import schema
from prime_backup.db.session import DbSession
from prime_backup.db.values import BlobStorageMethod
from prime_backup.exceptions import VerificationError
from prime_backup.types.blob_info import BlobInfo
from prime_backup.types.chunk_info import OffsetChunkInfo
from prime_backup.utils import blob_utils, chunk_utils
from prime_backup.utils import file_utils, hash_utils
from prime_backup.utils.bypass_io import BypassReader
from prime_backup.utils.io_types import SupportsReadBytes


class BlobChunksGetter(ABC):
	@abstractmethod
	def get(self, blob_id: int) -> List[OffsetChunkInfo]:
		...


class ThreadSafeBlobChunksGetter(BlobChunksGetter):
	def __init__(self, session: DbSession):
		self.session = session
		self.lock = threading.Lock()

	@override
	def get(self, blob_id: int) -> List[OffsetChunkInfo]:
		with self.lock:
			return [OffsetChunkInfo.of(oc) for oc in self.session.get_blob_chunks(blob_id)]


class _PeekReader:
	"""
	Exception raised in TarFile.addfile might nuke the whole remaining tar file, which is bad
	We read a few bytes from the stream, to *hopefully* trigger potential decompress exception in advanced,
	make it fail before affecting the actual tar file
	"""
	def __init__(self, file_obj: SupportsReadBytes, peek_size: int):
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


@dataclasses.dataclass(frozen=True)
class _OpenedChunk:
	reader: SupportsReadBytes
	verify_callback: Callable[[], None]


class _CombinedChunksReader:
	def __init__(self, chunks_gen: Generator[_OpenedChunk, None, None]):
		self.idx = 0
		self.chunks_gen = chunks_gen
		self.current: Optional[_OpenedChunk] = None
		self.reach_end = False

	def read(self, length: int = -1) -> bytes:
		if self.reach_end:
			return b''
		if self.current is None:
			if not self.__switch_to_next():
				return b''

		results: List[bytes] = []
		total_read = 0
		while length < 0 or total_read < length:
			to_read = length - total_read
			buf = self.current.reader.read(to_read)
			total_read += len(buf)
			results.append(buf)
			if len(buf) < to_read:
				self.current.verify_callback()
				if not self.__switch_to_next():
					break
		return b''.join(results)

	def __switch_to_next(self) -> bool:
		try:
			self.current = next(self.chunks_gen)
			return True
		except StopIteration:
			self.reach_end = True
			return False


class BlobExporter:
	def __init__(self, blob_chunks_getter: BlobChunksGetter, blob: BlobInfo, *, file_path: str, verify_blob: bool):
		self.blob_chunks_getter = blob_chunks_getter
		self.file_path = file_path
		self.blob = blob
		self.verify_blob = verify_blob

	def export_to_fs(self, output_path: Path):
		if self.blob.storage_method == BlobStorageMethod.direct:
			self.__export_to_fs_direct(output_path)
		elif self.blob.storage_method == BlobStorageMethod.chunked:
			self.__export_to_fs_chunked(output_path)
		else:
			raise ValueError('unsupported blob storage method {}'.format(self.blob.storage_method))

	def __export_to_fs_direct(self, output_path: Path):
		blob_path = blob_utils.get_blob_path(self.blob.hash)
		compressor = Compressor.create(self.blob.compress)
		if compressor.get_method() == CompressMethod.plain:
			file_utils.copy_file_fast(blob_path, output_path)
			if self.verify_blob:
				sah = hash_utils.calc_file_size_and_hash(output_path)
				self.__verify_exported_blob(sah.size, sah.hash)
		else:
			bypass_reader: Optional[BypassReader] = None
			with compressor.open_decompressed(blob_path) as f_in:
				with open(output_path, 'wb') as f_out:
					if self.verify_blob:
						bypass_reader = BypassReader(f_in, calc_hash=True)
						shutil.copyfileobj(bypass_reader, f_out)
					else:
						shutil.copyfileobj(f_in, f_out)
			if self.verify_blob and bypass_reader is not None:
				self.__verify_exported_blob(bypass_reader.get_read_len(), bypass_reader.get_hash())

	def __export_to_fs_chunked(self, output_path: Path):
		blob_chunks = self.blob_chunks_getter.get(self.blob.id)

		with open(output_path, 'wb') as f_out:
			for oc in blob_chunks:
				compressor = Compressor.create(oc.chunk.compress)
				chunk_path = chunk_utils.get_chunk_path(oc.chunk.hash)
				bypass_reader: Optional[BypassReader] = None
				with compressor.open_decompressed(chunk_path) as f_in:
					if self.verify_blob:
						bypass_reader = BypassReader(f_in, calc_hash=True, hash_method=chunk_utils.get_hash_method())
						shutil.copyfileobj(bypass_reader, f_out)
					else:
						shutil.copyfileobj(f_in, f_out)

				if self.verify_blob and bypass_reader is not None:
					self.__verify_exported_chunk(oc.chunk, bypass_reader.get_read_len(), bypass_reader.get_hash())

	def export_as_reader(self, reader_csm: Callable[[SupportsReadBytes], Any]):
		if self.blob.storage_method == BlobStorageMethod.direct:
			self.__export_as_reader_direct(reader_csm)
		elif self.blob.storage_method == BlobStorageMethod.chunked:
			self.__export_as_reader_chunked(reader_csm)
		else:
			raise ValueError('unsupported blob storage method {}'.format(self.blob.storage_method))

	def __export_as_reader_direct(self, reader_csm: Callable[[SupportsReadBytes], Any]):
		blob_path = blob_utils.get_blob_path(self.blob.hash)

		bypass_reader: Optional[BypassReader] = None
		with Compressor.create(self.blob.compress).open_decompressed(blob_path) as stream:
			peek_reader = _PeekReader(stream, 32 * 1024)
			peek_reader.peek()

			if self.verify_blob:
				bypass_reader = BypassReader(peek_reader, calc_hash=True)
				reader_csm(bypass_reader)
			else:
				reader_csm(peek_reader)

		if self.verify_blob and bypass_reader is not None:
			# notes: the read len is always <= info.size
			self.__verify_exported_blob(bypass_reader.get_read_len(), bypass_reader.get_hash())

	def __export_as_reader_chunked(self, reader_csm: Callable[[SupportsReadBytes], Any]):
		blob_chunks = self.blob_chunks_getter.get(self.blob.id)

		def open_chunk_gen() -> Generator[_OpenedChunk, None, None]:
			for oc in blob_chunks:
				compressor = Compressor.create(oc.chunk.compress)
				chunk_path = chunk_utils.get_chunk_path(oc.chunk.hash)

				with compressor.open_decompressed(chunk_path) as f_in:
					peek_reader = _PeekReader(f_in, 32 * 1024)
					peek_reader.peek()

					if self.verify_blob:
						def verify_callback(ck: schema.Chunk, br: BypassReader):
							self.__verify_exported_chunk(ck, br.get_read_len(), br.get_hash())

						bypass_reader = BypassReader(peek_reader, calc_hash=True, hash_method=chunk_utils.get_hash_method())
						yield _OpenedChunk(bypass_reader, functools.partial(verify_callback, oc.chunk, bypass_reader))
					else:
						yield _OpenedChunk(peek_reader, lambda: None)

				nonlocal exit_flag
				if exit_flag:
					break

		exit_flag = False
		chunk_gen = open_chunk_gen()
		try:
			reader_csm(_CombinedChunksReader(chunk_gen))
		finally:
			# ensure possible opened chunk file is closed
			exit_flag = True
			with contextlib.suppress(StopIteration):
				next(chunk_gen)

	def __verify_exported_blob(self, written_size: int, written_hash: str):
		self.__verify_exported_data(lambda: 'blob', self.blob.raw_size, self.blob.hash, written_size, written_hash)

	def __verify_exported_chunk(self, chunk: schema.Chunk, written_size: int, written_hash: str):
		self.__verify_exported_data(lambda: f'chunk {chunk.hash}', chunk.raw_size, chunk.hash, written_size, written_hash)

	def __verify_exported_data(self, what: Callable[[], str], expected_size: int, expected_hash: str, written_size: int, written_hash: str):
		if written_size != expected_size:
			raise VerificationError('raw size mismatched for {} ({}), expected {}, actual written {}'.format(self.file_path, what(), expected_size, written_size))
		if written_hash != expected_hash:
			raise VerificationError('hash mismatched for {} ({}), expected {}, actual written {}'.format(self.file_path, what(), expected_hash, written_hash))
