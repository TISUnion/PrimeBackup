import dataclasses
from abc import abstractmethod, ABC
from collections.abc import Iterable
from pathlib import Path
from typing import Iterator, TYPE_CHECKING, List, Generator, Union

from typing_extensions import override

from prime_backup.constants import chunk_constants
from prime_backup.types.hash_method import HashMethod, Hasher
from prime_backup.utils import blob_utils, misc_utils, hash_utils

if TYPE_CHECKING:
	import pyfastcdc


__HASH_METHOD = HashMethod[chunk_constants.HASH_METHOD]
__HASHER_FACTORY = __HASH_METHOD.value.create_hasher


def get_hash_method() -> HashMethod:
	return __HASH_METHOD


def create_hasher() -> Hasher:
	return __HASHER_FACTORY()


def get_chunk_store() -> Path:
	return blob_utils.get_blob_store() / '_chunks'


def get_chunk_path(h: str) -> Path:
	if len(h) <= 2:
		raise ValueError(f'hash {h!r} too short')

	return get_chunk_store() / h[:2] / h


def iterate_chunk_directories() -> Iterator[Path]:
	chunk_store = get_chunk_store()
	for i in range(0, 256):
		yield chunk_store / hex(i)[2:].rjust(2, '0')


def prepare_chunk_directories():
	for p in iterate_chunk_directories():
		p.mkdir(parents=True, exist_ok=True)


def should_chunk_blob(file_path: Union[str, Path], file_size: int) -> bool:
	from prime_backup.config.config import Config
	config = Config.get().backup
	return (
			config.cdc_enabled and
			file_size > 0 and file_size >= config.cdc_file_size_threshold and
			config.cdc_patterns_spec.match_file(file_path)
	)


def _create_cdc_chunker() -> 'pyfastcdc.FastCDC':
	from pyfastcdc import FastCDC
	return FastCDC(
		avg_size=chunk_constants.CDC_AVG_SIZE,
		min_size=chunk_constants.CDC_MIN_SIZE,
		max_size=chunk_constants.CDC_MAX_SIZE,
		normalized_chunking=1,
		seed=0,
	)


@dataclasses.dataclass(frozen=True)
class PrettyChunk:
	offset: int
	length: int
	hash: str


@dataclasses.dataclass(frozen=True)
class PrettyChunkWithData(PrettyChunk):
	data: memoryview


class _CDCChunker(ABC):
	def __init__(self, need_entire_file_hash: bool):
		self.need_entire_file_hash = need_entire_file_hash
		self.__entire_file_hasher = hash_utils.create_hasher()  # for the entire file hash, use config's hash method
		self.__file_size_sum = 0

	@abstractmethod
	def _do_cut(self) -> 'Iterable[pyfastcdc.Chunk]':
		...

	def cut(self) -> Generator[PrettyChunkWithData, None, None]:
		for cdc_chunk in self._do_cut():
			misc_utils.assert_true(cdc_chunk.length <= chunk_constants.CDC_MAX_SIZE, f'cdc cut chunk size too large: {cdc_chunk.length}')
			self.__file_size_sum += cdc_chunk.length

			if self.need_entire_file_hash:
				self.__entire_file_hasher.update(cdc_chunk.data)

			hasher = create_hasher()
			hasher.update(cdc_chunk.data)
			yield PrettyChunkWithData(
				offset=cdc_chunk.offset,
				length=cdc_chunk.length,
				hash=hasher.hexdigest(),
				data=cdc_chunk.data,
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


class FileChunker(_CDCChunker):
	def __init__(self, file_path: Path, need_entire_file_hash: bool = False):
		super().__init__(need_entire_file_hash)
		self.file_path = file_path

	@override
	def _do_cut(self) -> 'Iterable[pyfastcdc.Chunk]':
		return _create_cdc_chunker().cut_file(self.file_path)


class StreamChunker(_CDCChunker):
	def __init__(self, stream: 'pyfastcdc.BinaryStreamReader', need_entire_file_hash: bool = False):
		super().__init__(need_entire_file_hash)
		self.stream = stream

	@override
	def _do_cut(self) -> 'Iterable[pyfastcdc.Chunk]':
		return _create_cdc_chunker().cut_stream(self.stream)


def create_chunk_group_hash(chunk_hashes: 'Iterable[str]') -> str:
	hasher = create_hasher()
	hasher.update('\0'.join(chunk_hashes).encode('utf8'))
	return hasher.hexdigest()
