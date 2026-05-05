import dataclasses
from abc import abstractmethod, ABC
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, List, Generator, IO

from typing_extensions import override

from prime_backup.utils import misc_utils, hash_utils, chunk_utils

if TYPE_CHECKING:
	import pyfastcdc


# ======================== Chunk Data Classes ========================

@dataclasses.dataclass(frozen=True)
class PrettyChunk:
	offset: int
	length: int
	hash: str


@dataclasses.dataclass(frozen=True)
class PrettyChunkWithData(PrettyChunk):
	data: memoryview


# ======================== Abstract Chunker ========================

class _RawChunk:
	__slots__ = ('offset', 'length', 'data')

	offset: int
	length: int
	data: memoryview

	def __init__(self, offset: int, length: int, data: memoryview):
		self.offset = offset
		self.length = length
		self.data = data


class Chunker(ABC):
	"""Base class for all chunking strategies"""

	def __init__(self, need_entire_file_hash: bool):
		self.need_entire_file_hash = need_entire_file_hash
		self.__entire_file_hasher = hash_utils.create_hasher()  # for the entire file hash, use config's hash method
		self.__file_size_sum = 0

	@abstractmethod
	def _iter_raw_chunks(self) -> Iterable['_RawChunk']:
		...

	def cut(self) -> Generator[PrettyChunkWithData, None, None]:
		for raw_chunk in self._iter_raw_chunks():
			self.__file_size_sum += raw_chunk.length

			if self.need_entire_file_hash:
				self.__entire_file_hasher.update(raw_chunk.data)

			hasher = chunk_utils.create_hasher()
			hasher.update(raw_chunk.data)
			yield PrettyChunkWithData(
				offset=raw_chunk.offset,
				length=raw_chunk.length,
				hash=hasher.hexdigest(),
				data=raw_chunk.data,
			)

	def cut_all(self) -> List[PrettyChunk]:
		return [
			PrettyChunk(offset=c.offset, length=c.length, hash=c.hash)
			for c in self.cut()
		]

	def get_entire_file_hash(self) -> str:
		return self.__entire_file_hasher.hexdigest()

	def get_read_file_size(self) -> int:
		return self.__file_size_sum


# ======================== CDC Chunker ========================

@dataclasses.dataclass(frozen=True)
class CDCChunkerConfig:
	avg_size: int
	min_size: int
	max_size: int


class _CDCChunker(Chunker, ABC):
	def __init__(self, cfg: CDCChunkerConfig, need_entire_file_hash: bool):
		super().__init__(need_entire_file_hash)
		self.cfg = cfg

	def _create_cdc_engine(self) -> 'pyfastcdc.FastCDC':
		from pyfastcdc import FastCDC
		return FastCDC(
			avg_size=self.cfg.avg_size,
			min_size=self.cfg.min_size,
			max_size=self.cfg.max_size,
			normalized_chunking=1,
			seed=0,
		)


class CDCFileChunker(_CDCChunker):
	def __init__(self, cfg: CDCChunkerConfig, file_path: Path, need_entire_file_hash: bool = False):
		super().__init__(cfg, need_entire_file_hash)
		self.file_path = file_path

	@override
	def _iter_raw_chunks(self) -> Iterable[_RawChunk]:
		for c in self._create_cdc_engine().cut_file(self.file_path):
			misc_utils.assert_true(c.length <= self.cfg.max_size, f'cdc cut chunk size too large: {c.length}')
			yield _RawChunk(offset=c.offset, length=c.length, data=c.data)


class CDCStreamChunker(_CDCChunker):
	def __init__(self, cfg: CDCChunkerConfig, stream: 'pyfastcdc.BinaryStreamReader', need_entire_file_hash: bool = False):
		super().__init__(cfg, need_entire_file_hash)
		self.stream = stream

	@override
	def _iter_raw_chunks(self) -> Iterable[_RawChunk]:
		for c in self._create_cdc_engine().cut_stream(self.stream):
			misc_utils.assert_true(c.length <= self.cfg.max_size, f'cdc cut chunk size too large: {c.length}')
			yield _RawChunk(offset=c.offset, length=c.length, data=c.data)


# ======================== Fixed Size Chunker ========================


class _FixedSizeChunker(Chunker, ABC):
	def __init__(self, chunk_size: int, need_entire_file_hash: bool):
		super().__init__(need_entire_file_hash)
		self.chunk_size = chunk_size

	def _cut_stream_by_fixed_size(self, stream: IO[bytes]) -> Generator[_RawChunk, None, None]:
		offset = 0
		while True:
			buf = stream.read(self.chunk_size)
			if not buf:
				break
			yield _RawChunk(offset=offset, length=len(buf), data=memoryview(buf))
			offset += len(buf)


class FixedSizeFileChunker(_FixedSizeChunker):
	def __init__(self, chunk_size: int, file_path: Path, need_entire_file_hash: bool = False):
		super().__init__(chunk_size, need_entire_file_hash)
		self.file_path = file_path

	@override
	def _iter_raw_chunks(self) -> Iterable[_RawChunk]:
		with open(self.file_path, 'rb') as f:
			yield from self._cut_stream_by_fixed_size(f)


class FixedSizeStreamChunker(_FixedSizeChunker):
	def __init__(self, chunk_size: int, stream: IO[bytes], need_entire_file_hash: bool = False):
		super().__init__(chunk_size, need_entire_file_hash)
		self.stream = stream

	@override
	def _iter_raw_chunks(self) -> Iterable[_RawChunk]:
		yield from self._cut_stream_by_fixed_size(self.stream)
