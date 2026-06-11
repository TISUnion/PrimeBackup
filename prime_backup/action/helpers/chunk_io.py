import contextlib
from typing import Generator, Tuple, Optional

from prime_backup.action.helpers.pack_reader import PackReader, PackFileObjectPool
from prime_backup.compressors import Compressor
from prime_backup.types.chunk_info import ChunkInfo
from prime_backup.utils.bypass_io import BypassReader
from prime_backup.utils.io_types import SupportsReadBytes, SupportsReadAndSeek


class ChunkIO:
	def __init__(self, chunk: ChunkInfo, *, pack_file_obj_pool: Optional[PackFileObjectPool] = None):
		self.chunk = chunk
		self.pack_file_obj_pool = pack_file_obj_pool

	def __get_pack_id(self) -> int:
		if self.chunk.pack_entry.pack_id <= 0:
			raise ValueError('chunk {} has no pack id'.format(self.chunk.id))
		return self.chunk.pack_entry.pack_id

	@contextlib.contextmanager
	def open_raw(self) -> Generator[SupportsReadAndSeek, None, None]:
		with PackReader.open_entry(
				pack_id=self.__get_pack_id(),
				offset=self.chunk.pack_entry.offset,
				length=self.chunk.stored_size,
				file_obj_pool=self.pack_file_obj_pool
		) as reader:
			yield reader

	@contextlib.contextmanager
	def open_decompressed(self) -> Generator[SupportsReadBytes, None, None]:
		compressor = Compressor.create(self.chunk.compress)
		with self.open_raw() as raw:
			with compressor.decompress_stream(raw) as decompressed:
				yield decompressed

	@contextlib.contextmanager
	def open_decompressed_bypassed(self, *, calc_hash: bool = False) -> Generator[Tuple[BypassReader, SupportsReadBytes], None, None]:
		compressor = Compressor.create(self.chunk.compress)
		with self.open_raw() as raw:
			reader = BypassReader(raw, calc_hash=False)
			with compressor.decompress_stream(reader) as decompressed:
				if calc_hash:
					decompressed_reader = BypassReader(decompressed, calc_hash=True)
					yield reader, decompressed_reader
				else:
					yield reader, decompressed

	def read_raw(self) -> bytes:
		with self.open_raw() as reader:
			return reader.read()
