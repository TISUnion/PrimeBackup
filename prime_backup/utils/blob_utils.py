from pathlib import Path


def get_blob_store() -> Path:
	from prime_backup.config.config import Config
	return Config.get().storage_path / 'blobs'


def get_blob_path(h: str) -> Path:
	if len(h) <= 2:
		raise ValueError(f'hash {h!r} too short')

	return get_blob_store() / h[:2] / h


def prepare_blob_directories():
	for i in range(0, 256):
		p = get_blob_store() / hex(i)[2:].rjust(2, '0')
		p.mkdir(parents=True, exist_ok=True)
