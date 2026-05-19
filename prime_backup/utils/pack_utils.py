import functools
import hashlib
from pathlib import Path
from typing import Iterator


def get_pack_store() -> Path:
	from prime_backup.config.config import Config
	return Config.get().packs_path


@functools.lru_cache(maxsize=256)
def get_pack_file_name(pack_id: int) -> str:
	return hashlib.sha256(f'PBPK_{pack_id}'.encode('utf8')).hexdigest()


def get_pack_path(pack_id: int) -> Path:
	pack_file_name = get_pack_file_name(pack_id)
	return get_pack_store() / pack_file_name[:2] / pack_file_name


def iterate_pack_directories() -> Iterator[Path]:
	pack_store = get_pack_store()
	for i in range(0, 256):
		yield pack_store / hex(i)[2:].rjust(2, '0')


def prepare_pack_store():
	get_pack_store().mkdir(parents=True, exist_ok=True)


def prepare_pack_directories():
	prepare_pack_store()
	for p in iterate_pack_directories():
		p.mkdir(parents=True, exist_ok=True)


