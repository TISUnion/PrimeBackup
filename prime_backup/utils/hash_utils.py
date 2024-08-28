import dataclasses
from pathlib import Path
from typing import IO, Optional, TYPE_CHECKING

from prime_backup.utils.bypass_io import BypassReader

if TYPE_CHECKING:
	from prime_backup.types.hash_method import Hasher, HashMethod


def create_hasher(*, hash_method: Optional['HashMethod'] = None) -> 'Hasher':
	if hash_method is None:
		from prime_backup.db.access import DbAccess
		hash_method = DbAccess.get_hash_method()
	return hash_method.value.create_hasher()


_READ_BUF_SIZE = 128 * 1024


@dataclasses.dataclass(frozen=True)
class SizeAndHash:
	size: int
	hash: str


def calc_reader_size_and_hash(
		file_obj: IO[bytes], *,
		buf_size: int = _READ_BUF_SIZE,
		hash_method: Optional['HashMethod'] = None,
) -> SizeAndHash:
	reader = BypassReader(file_obj, True, hash_method=hash_method)
	while reader.read(buf_size):
		pass
	return SizeAndHash(reader.get_read_len(), reader.get_hash())


def calc_file_size_and_hash(path: Path, **kwargs) -> SizeAndHash:
	with open(path, 'rb') as f:
		return calc_reader_size_and_hash(f, **kwargs)


def calc_reader_hash(file_obj: IO[bytes], **kwargs) -> str:
	return calc_reader_size_and_hash(file_obj, **kwargs).hash


def calc_file_hash(path: Path, **kwargs) -> str:
	return calc_file_size_and_hash(path, **kwargs).hash


def calc_bytes_hash(buf: bytes) -> str:
	hasher = create_hasher()
	hasher.update(buf)
	return hasher.hexdigest()
