import dataclasses
import logging
import mmap
import os
from abc import abstractmethod, ABC
from pathlib import Path
from typing import TYPE_CHECKING, List, Generator, IO, Optional, Iterable, Dict, Iterator, Callable, Tuple, Type

from typing_extensions import override

from prime_backup.utils import misc_utils, hash_utils, chunk_utils, func_utils

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


class PrettyChunkSequence(ABC):
	@abstractmethod
	def __len__(self) -> int:
		...

	@abstractmethod
	def __iter__(self) -> Iterator[PrettyChunk]:
		...

	@abstractmethod
	def iter_hashes(self) -> Iterator[str]:
		...


@dataclasses.dataclass(frozen=True)
class SimplePrettyChunkSequence(PrettyChunkSequence):
	chunks: List[PrettyChunk]

	@override
	def __len__(self) -> int:
		return len(self.chunks)

	@override
	def __iter__(self) -> Iterator[PrettyChunk]:
		return iter(self.chunks)

	@override
	def iter_hashes(self) -> Iterator[str]:
		return (chunk.hash for chunk in self.chunks)


@dataclasses.dataclass(frozen=True)
class FixedPrettyChunkSequence(PrettyChunkSequence):
	file_size: int
	chunk_size: int
	one_hash_hex_len: int
	hash_hex_buf: str

	def __post_init__(self):
		if self.file_size < 0:
			raise ValueError('negative file size {}'.format(self.file_size))
		if self.chunk_size <= 0:
			raise ValueError('bad chunk size {}'.format(self.chunk_size))
		if self.one_hash_hex_len <= 0:
			raise ValueError('bad hash hex length {}'.format(self.one_hash_hex_len))
		if len(self.hash_hex_buf) != (expected_hash_len := len(self) * self.one_hash_hex_len):
			raise ValueError('bad fixed chunk hash hexes length {}, expected {}'.format(len(self.hash_hex_buf), expected_hash_len))

	@override
	def __len__(self) -> int:
		return (self.file_size + self.chunk_size - 1) // self.chunk_size

	def __hash_at(self, index: int) -> str:
		start = index * self.one_hash_hex_len
		end = start + self.one_hash_hex_len
		return self.hash_hex_buf[start:end]

	@override
	def __iter__(self) -> Iterator[PrettyChunk]:
		for index in range(len(self)):
			offset = index * self.chunk_size
			yield PrettyChunk(
				offset=offset,
				length=min(self.chunk_size, self.file_size - offset),
				hash=self.__hash_at(index),
			)

	@override
	def iter_hashes(self) -> Iterator[str]:
		for index in range(len(self)):
			yield self.__hash_at(index)


# ======================== Abstract Chunker ========================

_RawChunk = Tuple[int, int, memoryview, str]  # offset, length, data, hash


class Chunker(ABC):
	"""Base class for all chunking strategies"""

	def __init__(self, need_entire_file_hash: bool):
		self.need_entire_file_hash = need_entire_file_hash
		self.__entire_file_hasher = hash_utils.create_db_hasher() if need_entire_file_hash else None
		self.__file_size_sum = 0

	@abstractmethod
	def _iter_raw_chunks(self) -> Iterable['_RawChunk']:
		...

	def cut_all(self) -> List[PrettyChunk]:
		return list(self.cut())

	def cut(self) -> Generator[PrettyChunk, None, None]:
		for offset, length, chunk_data, chunk_hash in self.__do_cut():
			yield PrettyChunk(
				offset=offset,
				length=length,
				hash=chunk_hash,
			)

	def cut_with_data(self) -> Generator[PrettyChunkWithData, None, None]:
		"""
		The yielded PrettyChunkWithData.data is only guaranteed to be valid
		during the iteration of the cut_with_data() call.
		So consume it or copy it into a bytes object
		"""
		for offset, length, chunk_data, chunk_hash in self.__do_cut():
			yield PrettyChunkWithData(
				offset=offset,
				length=length,
				hash=chunk_hash,
				data=chunk_data,
			)

	def __do_cut(self) -> Generator[_RawChunk, None, None]:
		entire_file_hasher = self.__entire_file_hasher
		if need_entire_file_hash := self.need_entire_file_hash:
			assert entire_file_hasher is not None

		for offset, length, chunk_data, chunk_hash in self._iter_raw_chunks():
			self.__file_size_sum += length
			if need_entire_file_hash:
				entire_file_hasher.update(chunk_data)  # type: ignore[union-attr]
			yield offset, length, chunk_data, chunk_hash

	def cut_all_compact(self) -> PrettyChunkSequence:
		"""
		Same as cut_all(), but returns an object with more memory-efficient storage layout if possible
		"""
		return SimplePrettyChunkSequence(self.cut_all())

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


@func_utils.cached
def _get_fastcdc_class() -> Type['pyfastcdc.FastCDC']:
	from pyfastcdc import FastCDC
	return FastCDC


class _CDCChunker(Chunker, ABC):
	def __init__(self, cfg: FastCDCChunkerConfig, need_entire_file_hash: bool):
		super().__init__(need_entire_file_hash)
		self.cfg = cfg

	def _create_cdc_engine(self) -> 'pyfastcdc.FastCDC':
		return _get_fastcdc_class()(
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
			yield c.offset, c.length, c.data, chunk_utils.calc_bytes_hash(c.data)


class FastCDCStreamChunker(_CDCChunker):
	def __init__(self, cfg: FastCDCChunkerConfig, stream: 'pyfastcdc.BinaryStreamReader', need_entire_file_hash: bool = False):
		super().__init__(cfg, need_entire_file_hash)
		self.stream = stream

	@override
	def _iter_raw_chunks(self) -> Iterable[_RawChunk]:
		for c in self._create_cdc_engine().cut_stream(self.stream):
			misc_utils.assert_true(c.length <= self.cfg.max_size, f'cdc cut chunk size too large: {c.length}')
			yield c.offset, c.length, c.data, chunk_utils.calc_bytes_hash(c.data)


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
			yield offset, len(buf), memoryview(buf), chunk_utils.calc_bytes_hash(buf)
			offset += len(buf)

	@override
	def cut_all_compact(self) -> PrettyChunkSequence:
		hash_hex_list: List[str] = []
		hash_hex_len = chunk_utils.get_hash_method().value.hex_length
		for chunk in self.cut():
			if hash_hex_len != len(chunk.hash):
				raise ValueError('inconsistent chunk hash length: {} != {}'.format(len(chunk.hash), hash_hex_len))
			hash_hex_list.append(chunk.hash)
		return FixedPrettyChunkSequence(
			file_size=self.get_read_file_size(),
			chunk_size=self.chunk_size,
			one_hash_hex_len=hash_hex_len,
			hash_hex_buf=''.join(hash_hex_list),
		)


class _MmapFileIterator:
	__data: memoryview
	__closer: Callable[[], None]

	def __init__(self, file_path: Path):
		self.__file_size = os.path.getsize(file_path)
		if self.__file_size == 0:
			self.__data = memoryview(b'')
			self.__closer = lambda: None
		else:
			file = open(file_path, 'rb')
			self.__data = memoryview(mmap.mmap(file.fileno(), length=self.__file_size, access=mmap.ACCESS_READ))
			self.__closer = file.close

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.__data = memoryview(b'')
		self.__closer()

	def iterate(self, chunk_size: int) -> Iterable[Tuple[int, memoryview]]:
		offset = 0
		while offset < self.__file_size:
			buf = memoryview(self.__data[offset: offset + chunk_size])
			yield offset, buf
			offset += len(buf)


class FixedSizeFileChunker(_FixedSizeChunker):
	def __init__(self, chunk_size: int, file_path: Path, need_entire_file_hash: bool = False):
		super().__init__(chunk_size, need_entire_file_hash)
		self.file_path = file_path

	@override
	def _iter_raw_chunks(self) -> Iterable[_RawChunk]:
		with _MmapFileIterator(self.file_path) as mmap_iter:
			for offset, buf in mmap_iter.iterate(self.chunk_size):
				yield offset, len(buf), buf, chunk_utils.calc_bytes_hash(buf)


class LegacyFixedSizeFileChunker(_FixedSizeChunker):
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
			chunk_data = data[start:end]
			chunk_hash = chunk_hashes[idx] if chunk_hashes is not None else chunk_utils.calc_bytes_hash(chunk_data)
			yield small_offset, self.SMALL_CHUNK_SIZE, chunk_data, chunk_hash

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

		with _MmapFileIterator(self.file_path) as mmap_iter:
			for offset, data in mmap_iter.iterate(self.BIG_CHUNK_SIZE):
				window_count += 1
				if len(data) != self.BIG_CHUNK_SIZE:
					tail_window_count += 1
					emitted_tail_count += 1
					yield offset, len(data), data, chunk_utils.calc_bytes_hash(data)
					continue

				if (previous_big_chunk := self.__get_previous_big_chunk(offset)) is not None:
					current_big_hash = self.__calc_chunk_hash(data)
					if current_big_hash == previous_big_chunk.hash:
						previous_big_reused_count += 1
						emitted_big_count += 1
						yield offset, self.BIG_CHUNK_SIZE, data, current_big_hash
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
						yield offset, self.BIG_CHUNK_SIZE, data, chunk_utils.calc_bytes_hash(data)
					else:
						previous_small_kept_count += 1
						emitted_small_count += self.SMALL_CHUNK_COUNT
						yield from self.__iter_small_chunks(offset, data, current_small_hashes)
				else:
					fallback_big_count += 1
					emitted_big_count += 1
					yield offset, self.BIG_CHUNK_SIZE, data, chunk_utils.calc_bytes_hash(data)

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
