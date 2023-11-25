import datetime
import shutil
from pathlib import Path
from typing import Union, Callable

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


def get_blob_path(h: str) -> Path:
	if len(h) <= 2:
		raise ValueError(h)

	from xbackup.config.config import Config
	blob_dir = Path(Config.get().storage_path) / 'blobs' / h[:2]
	if not blob_dir.is_dir():
		blob_dir.mkdir(parents=True, exist_ok=True)
	return blob_dir / h


def timestamp_to_local_date(timestamp_ns: int) -> str:
	timestamp_seconds = timestamp_ns / 1e9
	dt = datetime.datetime.fromtimestamp(timestamp_seconds)
	local_tz = datetime.datetime.now().astimezone().tzinfo
	return str(dt.astimezone(local_tz))


def assert_true(expr: bool, msg: Union[str, Callable[[], str]]):
	if not expr:
		if callable(msg):
			msg = msg()
		raise AssertionError(msg)


def copy_file_fast(src_path: Path, dst_path: Path):
	shutil.copyfile(src_path, dst_path, follow_symlinks=False)
