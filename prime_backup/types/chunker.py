import dataclasses
import logging
from abc import abstractmethod, ABC
from pathlib import Path
from typing import TYPE_CHECKING, List, Generator, IO, Optional, Iterable, Dict

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
	__slots__ = ('offset', 'length', 'data', 'hash')

	offset: int
	length: int
	data: memoryview
	hash: Optional[str]

	def __init__(self, *, offset: int, length: int, data: memoryview, hash: Optional[str] = None):
		self.offset = offset
		self.length = length
		self.data = data
		self.hash = hash


class Chunker(ABC):
	"""Base class for all chunking strategies"""

	def __init__(self, need_entire_file_hash: bool):
		self.need_entire_file_hash = need_entire_file_hash
		self.__entire_file_hasher = hash_utils.create_hasher() if need_entire_file_hash else None
		self.__file_size_sum = 0

	@abstractmethod
	def _iter_raw_chunks(self) -> Iterable['_RawChunk']:
		...

	def cut(self) -> Generator[PrettyChunkWithData, None, None]:
		for raw_chunk in self._iter_raw_chunks():
			self.__file_size_sum += raw_chunk.length

			if self.need_entire_file_hash:
				assert self.__entire_file_hasher is not None
				self.__entire_file_hasher.update(raw_chunk.data)

			chunk_hash = raw_chunk.hash
			if chunk_hash is None:
				hasher = chunk_utils.create_hasher()
				hasher.update(raw_chunk.data)
				chunk_hash = hasher.hexdigest()
			yield PrettyChunkWithData(
				offset=raw_chunk.offset,
				length=raw_chunk.length,
				hash=chunk_hash,
				data=raw_chunk.data,
			)

	def cut_all(self) -> List[PrettyChunk]:
		return [
			PrettyChunk(offset=c.offset, length=c.length, hash=c.hash)
			for c in self.cut()
		]

	def get_entire_file_hash(self) -> str:
		if self.__entire_file_hasher is None:
			raise RuntimeError('entire file hash is not calculated')
		return self.__entire_file_hasher.hexdigest()

	def get_read_file_size(self) -> int:
		return self.__file_size_sum


# ======================== FastCDC Chunker ========================

@dataclasses.dataclass(frozen=True)
class FastCDCChunkerConfig:
	avg_size: int
	min_size: int
	max_size: int


class _CDCChunker(Chunker, ABC):
	def __init__(self, cfg: FastCDCChunkerConfig, need_entire_file_hash: bool):
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


class FastCDCFileChunker(_CDCChunker):
	def __init__(self, cfg: FastCDCChunkerConfig, file_path: Path, need_entire_file_hash: bool = False):
		super().__init__(cfg, need_entire_file_hash)
		self.file_path = file_path

	@override
	def _iter_raw_chunks(self) -> Iterable[_RawChunk]:
		for c in self._create_cdc_engine().cut_file(self.file_path):
			misc_utils.assert_true(c.length <= self.cfg.max_size, f'cdc cut chunk size too large: {c.length}')
			yield _RawChunk(offset=c.offset, length=c.length, data=c.data)


class FastCDCStreamChunker(_CDCChunker):
	def __init__(self, cfg: FastCDCChunkerConfig, stream: 'pyfastcdc.BinaryStreamReader', need_entire_file_hash: bool = False):
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


# ======================== Fixed Auto Chunker ========================


class FixedAutoFileChunker(Chunker):
	BIG_CHUNK_SIZE = 128 * 1024
	SMALL_CHUNK_SIZE = 4 * 1024
	SMALL_CHUNK_COUNT = BIG_CHUNK_SIZE // SMALL_CHUNK_SIZE

	def __init__(
			self,
			file_path: Path,
			previous_chunks: Optional[Iterable[PrettyChunk]] = None,
			need_entire_file_hash: bool = False,
	):
		super().__init__(need_entire_file_hash)
		self.file_path = file_path
		self.previous_chunks_by_offset: Dict[int, PrettyChunk] = {
			chunk.offset: chunk for chunk in previous_chunks or []
		}

	@staticmethod
	def __calc_chunk_hash(data: memoryview) -> str:
		hasher = chunk_utils.create_hasher()
		hasher.update(data)
		return hasher.hexdigest()

	def __get_previous_big_chunk(self, offset: int) -> Optional[PrettyChunk]:
		chunk = self.previous_chunks_by_offset.get(offset)
		if chunk is not None and chunk.length == self.BIG_CHUNK_SIZE:
			return chunk
		return None

	def __get_previous_small_chunks(self, offset: int) -> Optional[List[PrettyChunk]]:
		chunks: List[PrettyChunk] = []
		for idx in range(self.SMALL_CHUNK_COUNT):
			chunk_offset = offset + idx * self.SMALL_CHUNK_SIZE
			chunk = self.previous_chunks_by_offset.get(chunk_offset)
			if chunk is None or chunk.length != self.SMALL_CHUNK_SIZE:
				return None
			chunks.append(chunk)
		return chunks

	def __iter_small_chunks(self, offset: int, data: memoryview, chunk_hashes: Optional[List[str]]) -> Generator[_RawChunk, None, None]:
		for idx in range(self.SMALL_CHUNK_COUNT):
			small_offset = offset + idx * self.SMALL_CHUNK_SIZE
			start = idx * self.SMALL_CHUNK_SIZE
			end = start + self.SMALL_CHUNK_SIZE
			yield _RawChunk(
				offset=small_offset,
				length=self.SMALL_CHUNK_SIZE,
				data=data[start:end],
				hash=chunk_hashes[idx] if chunk_hashes is not None else None,
			)

	@override
	def _iter_raw_chunks(self) -> Iterable[_RawChunk]:
		window_count = 0
		tail_window_count = 0
		emitted_big_count = 0
		emitted_small_count = 0
		emitted_tail_count = 0
		fallback_big_count = 0
		previous_big_reused_count = 0
		previous_big_split_count = 0
		previous_small_merged_count = 0
		previous_small_kept_count = 0

		with open(self.file_path, 'rb') as f:
			offset = 0
			while True:
				buf = f.read(self.BIG_CHUNK_SIZE)
				if not buf:
					break

				window_count += 1
				data = memoryview(buf)
				if len(buf) != self.BIG_CHUNK_SIZE:
					tail_window_count += 1
					emitted_tail_count += 1
					yield _RawChunk(offset=offset, length=len(buf), data=data)
					offset += len(buf)
					continue

				if (previous_big_chunk := self.__get_previous_big_chunk(offset)) is not None:
					current_big_hash = self.__calc_chunk_hash(data)
					if current_big_hash == previous_big_chunk.hash:
						previous_big_reused_count += 1
						emitted_big_count += 1
						yield _RawChunk(offset=offset, length=self.BIG_CHUNK_SIZE, data=data, hash=current_big_hash)
					else:
						previous_big_split_count += 1
						emitted_small_count += self.SMALL_CHUNK_COUNT
						yield from self.__iter_small_chunks(offset, data, None)
				elif (previous_small_chunks := self.__get_previous_small_chunks(offset)) is not None:
					current_small_hashes = [
						self.__calc_chunk_hash(data[idx * self.SMALL_CHUNK_SIZE:(idx + 1) * self.SMALL_CHUNK_SIZE])
						for idx in range(self.SMALL_CHUNK_COUNT)
					]
					changed_count = sum(
						1
						for previous_chunk, current_hash in zip(previous_small_chunks, current_small_hashes)
						if previous_chunk.hash != current_hash
					)
					if changed_count == 0:
						previous_small_merged_count += 1
						emitted_big_count += 1
						yield _RawChunk(offset=offset, length=self.BIG_CHUNK_SIZE, data=data)
					else:
						previous_small_kept_count += 1
						emitted_small_count += self.SMALL_CHUNK_COUNT
						yield from self.__iter_small_chunks(offset, data, current_small_hashes)
				else:
					fallback_big_count += 1
					emitted_big_count += 1
					yield _RawChunk(offset=offset, length=self.BIG_CHUNK_SIZE, data=data)

				offset += len(buf)

		from prime_backup import logger
		log = logger.get()
		has_changed_windows = previous_big_split_count > 0 or previous_small_kept_count > 0
		if has_changed_windows and log.isEnabledFor(logging.DEBUG):
			log.debug('fixed_auto stats {!r}: windows {}+{}, chunks big/small/tail {}/{}/{}, big reuse/split/fallback {}/{}/{}, small merge/keep {}/{}'.format(
				self.file_path.as_posix(),
				window_count, tail_window_count,
				emitted_big_count, emitted_small_count, emitted_tail_count,
				previous_big_reused_count, previous_big_split_count, fallback_big_count,
				previous_small_merged_count, previous_small_kept_count,
			))
