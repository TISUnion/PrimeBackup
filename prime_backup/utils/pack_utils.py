import uuid
from pathlib import Path
from typing import Iterator


def get_pack_store() -> Path:
	from prime_backup.config.config import Config
	return Config.get().packs_path


def get_pack_path(pack_name: str) -> Path:
	return get_pack_store() / pack_name[:2] / pack_name


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


def generate_pack_name() -> str:
	return uuid.uuid4().hex
