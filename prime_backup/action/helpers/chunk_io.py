import contextlib
from typing import Generator, Tuple

from prime_backup.action.helpers.pack_reader import PackReader
from prime_backup.compressors import Compressor
from prime_backup.types.chunk_info import ChunkInfo
from prime_backup.utils.bypass_io import BypassReader
from prime_backup.utils.io_types import SupportsReadBytes


class ChunkIO:
	def __init__(self, chunk: ChunkInfo):
		self.chunk = chunk

	def __get_pack_name(self) -> str:
		if len(self.chunk.pack_entry.pack_name) == 0:
			raise ValueError('chunk {} has no pack name'.format(self.chunk.id))
		return self.chunk.pack_entry.pack_name

	@contextlib.contextmanager
	def open_raw(self) -> Generator[SupportsReadBytes, None, None]:
		with PackReader.open_entry(self.__get_pack_name(), self.chunk.pack_entry.pack_offset, self.chunk.stored_size) as reader:
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
