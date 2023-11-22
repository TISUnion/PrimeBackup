from pathlib import Path

import xxhash


def calc_file_hash(path: Path, *, buf_size: int = 64 * 1024):
	with open(path, 'rb') as f:
		hasher = xxhash.xxh128()
		while True:
			chunk = f.read(buf_size)
			if not chunk:
				break
			hasher.update(chunk)
		return hasher.hexdigest()
