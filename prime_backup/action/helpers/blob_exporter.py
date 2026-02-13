import dataclasses
import functools
import shutil
from pathlib import Path
from typing import Optional, Callable, Any, Dict, IO, ContextManager, List

from prime_backup.compressors import CompressMethod
from prime_backup.compressors import Compressor
from prime_backup.db import schema
from prime_backup.db.session import DbSession
from prime_backup.db.values import BlobStorageMethod
from prime_backup.exceptions import VerificationError
from prime_backup.types.blob_info import BlobInfo
from prime_backup.types.file_info import FileInfo
from prime_backup.utils import blob_utils, chunk_utils
from prime_backup.utils import file_utils, hash_utils
from prime_backup.utils.bypass_io import BypassReader
from prime_backup.utils.io_types import SupportsReadBytes


class _PeekReader:
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


@dataclasses.dataclass
class _OpenedChunk:
	reader: SupportsReadBytes
	verify_callback: Callable[[], None]


class _CombinedChunksReader:
	def __init__(self, chunks: List[_OpenedChunk]):
		self.idx = 0
		self.chunks = chunks

	def read(self, length: int = -1) -> bytes:
		if self.idx >= len(self.chunks):
			return b''

		results: List[bytes] = []
		total_read = 0
		while (length < 0 or total_read < length) or self.idx < len(self.chunks):
			to_read = length - total_read
			buf = self.chunks[self.idx].reader.read(to_read)
			total_read += len(buf)
			results.append(buf)
			if len(buf) < to_read:
				self.idx += 1
		return b''.join(results)


class BlobExporter:
	def __init__(self, session: DbSession, blob: BlobInfo, *, file_path: str, verify_blob: bool):
		self.session = session
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
		blob_chunks = self.session.list_blob_chunks(self.blob.id)

		with open(output_path, 'wb') as f_out:
			for chunk in blob_chunks.values():
				compressor = Compressor.create(chunk.compress)
				chunk_path = chunk_utils.get_chunk_path(chunk.hash)
				bypass_reader: Optional[BypassReader] = None
				with compressor.open_decompressed(chunk_path) as f_in:
					if self.verify_blob:
						bypass_reader = BypassReader(f_in, calc_hash=True)
						shutil.copyfileobj(bypass_reader, f_out)
					else:
						shutil.copyfileobj(f_in, f_out)

				if self.verify_blob and bypass_reader is not None:
					self.__verify_exported_chunk(chunk, bypass_reader.get_read_len(), bypass_reader.get_hash())

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
			# Exception raised in TarFile.addfile might nuke the whole remaining tar file, which is bad
			# We read a few bytes from the stream, to *hopefully* trigger potential decompress exception in advanced,
			# make it fail before affecting the actual tar file
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
		blob_chunks = self.session.list_blob_chunks(self.blob.id)

		chunk_cm_list: List[ContextManager[IO[bytes]]] = []
		try:
			to_read_chunks: List[_OpenedChunk] = []
			for chunk in blob_chunks.values():
				compressor = Compressor.create(chunk.compress)
				chunk_path = chunk_utils.get_chunk_path(chunk.hash)
				bypass_reader: Optional[BypassReader] = None

				f_in_cm: ContextManager[IO[bytes]] = compressor.open_decompressed(chunk_path)
				f_in = f_in_cm.__enter__()
				chunk_cm_list.append(f_in_cm)
				if self.verify_blob:
					def verify_callback(br: BypassReader):
						self.__verify_exported_chunk(chunk, br.get_read_len(), br.get_hash())

					bypass_reader = BypassReader(f_in, calc_hash=True)
					to_read_chunks.append(_OpenedChunk(bypass_reader, functools.partial(verify_callback, bypass_reader)))
				else:
					to_read_chunks.append(_OpenedChunk(bypass_reader, lambda: None))

			reader_csm(_CombinedChunksReader(to_read_chunks))
		finally:
			for cm in chunk_cm_list:
				# TODO: pass exception values?
				cm.__exit__(None, None, None)

	def __verify_exported_blob(self, written_size: int, written_hash: str):
		self.__verify_exported_data(self.blob.raw_size, self.blob.hash, written_size, written_hash)

	def __verify_exported_chunk(self, chunk: schema.Chunk, written_size: int, written_hash: str):
		self.__verify_exported_data(chunk.raw_size, chunk.hash, written_size, written_hash)

	def __verify_exported_data(self, expected_size: int, expected_hash: str, written_size: int, written_hash: str):
		if written_size != expected_size:
			raise VerificationError('raw size mismatched for {}, expected {}, actual written {}'.format(self.file_path, expected_size, written_size))
		if written_hash != expected_hash:
			raise VerificationError('hash mismatched for {}, expected {}, actual written {}'.format(self.file_path, expected_hash, written_hash))
