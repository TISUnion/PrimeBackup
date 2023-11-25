from pathlib import Path

from xbackup.config.config import Config
from xbackup.config.types import Hasher


def create_hasher() -> 'Hasher':
	return Config.get().backup.hash_method.value.create_hasher()


def calc_file_hash(path: Path, *, buf_size: int = 64 * 1024) -> str:
	with open(path, 'rb') as f:
		hasher = create_hasher()
		while True:
			chunk = f.read(buf_size)
			if not chunk:
				break
			hasher.update(chunk)
		return hasher.hexdigest()


def calc_bytes_hash(buf: bytes) -> str:
	hasher = create_hasher()
	hasher.update(buf)
	return hasher.hexdigest()
