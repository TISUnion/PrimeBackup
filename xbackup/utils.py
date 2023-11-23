import datetime
from pathlib import Path

import xxhash


def calc_file_hash(path: Path, *, buf_size: int = 64 * 1024) -> str:
	with open(path, 'rb') as f:
		hasher = xxhash.xxh128()
		while True:
			chunk = f.read(buf_size)
			if not chunk:
				break
			hasher.update(chunk)
		return hasher.hexdigest()


def get_blob_path(h: str) -> Path:
	if len(h) <= 2:
		raise ValueError(h)

	from xbackup.config.config import Config
	blob_dir = Path(Config.get().storage_path) / 'blobs' / h[:2]
	if not blob_dir.is_dir():
		blob_dir.mkdir(parents=True, exist_ok=True)
	return blob_dir / h


def timestamp_to_local_date(timestamp_ms: int) -> str:
	timestamp_seconds = timestamp_ms / 1000.0
	dt = datetime.datetime.fromtimestamp(timestamp_seconds)
	local_tz = datetime.datetime.now().astimezone().tzinfo
	return str(dt.astimezone(local_tz))
