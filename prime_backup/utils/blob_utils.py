from pathlib import Path
from typing import Iterator


def get_blob_store() -> Path:
	from prime_backup.config.config import Config
	return Config.get().blobs_path


def get_blob_path(h: str) -> Path:
	if len(h) <= 2:
		raise ValueError(f'hash {h!r} too short')

	return get_blob_store() / h[:2] / h


def iterate_blob_directories() -> Iterator[Path]:
	blob_store = get_blob_store()
	for i in range(0, 256):
		yield blob_store / hex(i)[2:].rjust(2, '0')


def prepare_blob_directories():
	for p in iterate_blob_directories():
		p.mkdir(parents=True, exist_ok=True)
