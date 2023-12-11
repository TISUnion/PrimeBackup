from pathlib import Path
from typing import NamedTuple, IO

from prime_backup.types.hash_method import Hasher
from prime_backup.utils.bypass_io import BypassReader


def create_hasher() -> 'Hasher':
	from prime_backup.db.access import DbAccess
	return DbAccess.get_hash_method().value.create_hasher()


_READ_BUF_SIZE = 128 * 1024


class SizeAndHash(NamedTuple):
	size: int
	hash: str


def calc_reader_size_and_hash(file_obj: IO[bytes], *, buf_size: int = _READ_BUF_SIZE) -> SizeAndHash:
	reader = BypassReader(file_obj, True)
	while reader.read(buf_size):
		pass
	return SizeAndHash(reader.get_read_len(), reader.get_hash())


def calc_file_size_and_hash(path: Path, *, buf_size: int = _READ_BUF_SIZE) -> SizeAndHash:
	with open(path, 'rb') as f:
		return calc_reader_size_and_hash(f, buf_size=buf_size)


def calc_reader_hash(file_obj: IO[bytes], *, buf_size: int = _READ_BUF_SIZE) -> str:
	return calc_reader_size_and_hash(file_obj, buf_size=buf_size).hash


def calc_file_hash(path: Path, *, buf_size: int = _READ_BUF_SIZE) -> str:
	return calc_file_size_and_hash(path, buf_size=buf_size).hash


def calc_bytes_hash(buf: bytes) -> str:
	hasher = create_hasher()
	hasher.update(buf)
	return hasher.hexdigest()
