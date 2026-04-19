from collections.abc import Iterable
from pathlib import Path
from typing import Iterator

from prime_backup.constants import chunk_constants
from prime_backup.types.chunk_method import ChunkMethod
from prime_backup.types.hash_method import HashMethod, Hasher
from prime_backup.utils import blob_utils
from prime_backup.utils.path_like import PathLike

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


def should_chunk_blob(file_path: PathLike, file_size: int) -> bool:
	return ChunkMethod.get_for_file(file_path, file_size) is not None


def create_chunk_group_hash(chunk_hashes: 'Iterable[str]') -> str:
	hasher = create_hasher()
	hasher.update('\0'.join(chunk_hashes).encode('utf8'))
	return hasher.hexdigest()
