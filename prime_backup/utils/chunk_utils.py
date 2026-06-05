from collections.abc import Iterable

from prime_backup.constants import chunk_constants
from prime_backup.types.hash_method import HashMethod
from prime_backup.utils import hash_utils

_CHUNK_GROUP_HASH_METHOD = HashMethod[chunk_constants.CHUNK_GROUP_HASH_METHOD]
__create_chunk_group_hasher = _CHUNK_GROUP_HASH_METHOD.value.create_hasher


def get_chunk_group_hash_method() -> HashMethod:
	return _CHUNK_GROUP_HASH_METHOD


def create_chunk_group_hash(chunk_hashes: 'Iterable[str]') -> str:
	return __create_chunk_group_hasher('\0'.join(chunk_hashes).encode('utf8')).hexdigest()


# Currently chunk's hash method is the same as the default DB hash method
# so here comes the function alias
get_hash_method = hash_utils.get_configured_hash_method
create_hasher = hash_utils.create_db_hasher
calc_bytes_hash = hash_utils.calc_bytes_hash
