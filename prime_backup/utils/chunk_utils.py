import dataclasses
from collections.abc import Iterable
from pathlib import Path
from typing import Iterator, TYPE_CHECKING, List, Union, Generator

from prime_backup.constants import chunk_constants
from prime_backup.types.hash_method import HashMethod, Hasher
from prime_backup.utils import blob_utils, misc_utils

if TYPE_CHECKING:
	import pyfastcdc

__HASHER_FACTORY = HashMethod[chunk_constants.HASH_METHOD].value.create_hasher


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


def _cdc_cut_file(file_path: Path) -> 'Iterable[pyfastcdc.Chunk]':
	from pyfastcdc import FastCDC
	cdc = FastCDC(
		avg_size=chunk_constants.CDC_AVG_SIZE,
		min_size=chunk_constants.CDC_MIN_SIZE,
		max_size=chunk_constants.CDC_MAX_SIZE,
		normalized_chunking=1,
		seed=0,
	)
	yield from cdc.cut_file(file_path)


@dataclasses.dataclass(frozen=True)
class PrettyChunk:
	offset: int
	length: int
	hash: str


class FileChunker:
	def __init__(
			self,
			file_path: Path,
			need_entire_file_hash: bool = False,
	):
		self.file_path = file_path
		self.need_entire_file_hash = need_entire_file_hash
		self.__entire_file_hasher = create_hasher()
		self.__file_size_sum = 0

	def cut(self) -> Generator[PrettyChunk, None, None]:
		for cdc_chunk in _cdc_cut_file(self.file_path):
			misc_utils.assert_true(cdc_chunk.length <= chunk_constants.CDC_MAX_SIZE, f'cdc cut chunk size too large: {cdc_chunk.length}')
			self.__file_size_sum += cdc_chunk.length

			if self.need_entire_file_hash:
				self.__entire_file_hasher.update(cdc_chunk.data)

			hasher = create_hasher()
			hasher.update(cdc_chunk.data)
			yield PrettyChunk(
				offset=cdc_chunk.offset,
				length=cdc_chunk.length,
				hash=hasher.hexdigest(),
			)

	def cut_all(self) -> List[PrettyChunk]:
		return list(self.cut())

	def get_entire_file_hash(self) -> str:
		return self.__entire_file_hasher.hexdigest()

	def get_read_file_size(self) -> int:
		return self.__file_size_sum


def create_chunk_group_hash(chunk_hashes: 'Iterable[str]') -> str:
	hasher = create_hasher()
	hasher.update('\0'.join(chunk_hashes).encode('utf8'))
	return hasher.hexdigest()
