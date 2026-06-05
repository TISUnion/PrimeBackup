import dataclasses
from pathlib import Path
from typing import Optional, TYPE_CHECKING, Callable

from prime_backup.utils import func_utils
from prime_backup.utils.io_types import SupportsReadBytes

if TYPE_CHECKING:
	from prime_backup.types.hash_method import Hasher, HashMethod


class _CachedDbAccess:
	@staticmethod
	@func_utils.cached
	def get_hash_method_func() -> Callable[[], 'HashMethod']:
		from prime_backup.db.access import DbAccess
		# noinspection PyProtectedMember
		return DbAccess._get_hash_method_no_check

	@staticmethod
	@func_utils.cached
	def create_hasher_func() -> Callable[[], 'Hasher']:
		from prime_backup.db.access import DbAccess
		# noinspection PyProtectedMember
		return DbAccess._create_hasher_no_check


def get_configured_hash_method() -> 'HashMethod':
	return _CachedDbAccess.get_hash_method_func()()


def create_db_hasher() -> 'Hasher':
	return  _CachedDbAccess.create_hasher_func()()


def create_hasher(*, hash_method: Optional['HashMethod'] = None) -> 'Hasher':
	if hash_method is None:
		return create_db_hasher()
	return hash_method.value.create_hasher()


_READ_BUF_SIZE = 128 * 1024


@dataclasses.dataclass(frozen=True)
class SizeAndHash:
	size: int
	hash: str


def calc_reader_size_and_hash(
		file_obj: SupportsReadBytes, *,
		buf_size: int = _READ_BUF_SIZE,
		hash_method: Optional['HashMethod'] = None,
) -> SizeAndHash:
	from prime_backup.utils.bypass_io import BypassReader
	reader = BypassReader(file_obj, calc_hash=True, hash_method=hash_method)
	while reader.read(buf_size):
		pass
	return SizeAndHash(reader.get_read_len(), reader.get_hash())


def calc_file_size_and_hash(path: Path, **kwargs) -> SizeAndHash:
	with open(path, 'rb') as f:
		return calc_reader_size_and_hash(f, **kwargs)


def calc_reader_hash(file_obj: SupportsReadBytes, **kwargs) -> str:
	return calc_reader_size_and_hash(file_obj, **kwargs).hash


def calc_file_hash(path: Path, **kwargs) -> str:
	return calc_file_size_and_hash(path, **kwargs).hash


def calc_bytes_hash(buf: bytes, hasher: Optional['Hasher'] = None) -> str:
	if hasher is None:
		hasher = create_db_hasher()
	hasher.update(buf)
	return hasher.hexdigest()
