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
