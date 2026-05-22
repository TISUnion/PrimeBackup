from collections.abc import Iterable

from prime_backup.constants import chunk_constants
from prime_backup.types.hash_method import HashMethod, Hasher
from prime_backup.utils import hash_utils

_CHUNK_GROUP_HASH_METHOD = HashMethod[chunk_constants.CHUNK_GROUP_HASH_METHOD]


def get_hash_method() -> HashMethod:
	return hash_utils.get_configured_hash_method()


def create_hasher() -> Hasher:
	return hash_utils.create_hasher()


def get_chunk_group_hash_method() -> HashMethod:
	return _CHUNK_GROUP_HASH_METHOD


def create_chunk_group_hash(chunk_hashes: 'Iterable[str]') -> str:
	hasher = _CHUNK_GROUP_HASH_METHOD.value.create_hasher()
	hasher.update('\0'.join(chunk_hashes).encode('utf8'))
	return hasher.hexdigest()
