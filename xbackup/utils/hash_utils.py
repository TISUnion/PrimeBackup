from pathlib import Path
from typing import Tuple

from xbackup.config.config import Config
from xbackup.config.types import Hasher
from xbackup.utils.bypass_reader import ByPassReader


def create_hasher() -> 'Hasher':
	return Config.get().backup.hash_method.value.create_hasher()


_READ_BUF_SIZE = 128 * 1024


def calc_file_size_and_hash(path: Path, *, buf_size: int = _READ_BUF_SIZE) -> Tuple[int, str]:
	with open(path, 'rb') as f:
		reader = ByPassReader(f, True)
		while reader.read(buf_size):
			pass
		return reader.get_read_len(), reader.get_hash()


def calc_file_hash(path: Path, *, buf_size: int = _READ_BUF_SIZE) -> str:
	return calc_file_size_and_hash(path, buf_size=buf_size)[1]


def calc_bytes_hash(buf: bytes) -> str:
	hasher = create_hasher()
	hasher.update(buf)
	return hasher.hexdigest()
