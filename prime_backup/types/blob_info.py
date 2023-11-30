from pathlib import Path
from typing import NamedTuple

from prime_backup.compressors import CompressMethod
from prime_backup.db import schema


class BlobInfo(NamedTuple):
	hash: str
	compress: CompressMethod
	raw_size: int
	stored_size: int

	@classmethod
	def of(cls, blob: schema.Blob) -> 'BlobInfo':
		"""
		Notes: should be inside a session
		"""
		return BlobInfo(
			hash=blob.hash,
			compress=CompressMethod[blob.compress],
			raw_size=blob.raw_size,
			stored_size=blob.stored_size,
		)

	@property
	def blob_path(self) -> Path:
		from prime_backup.utils import blob_utils
		return blob_utils.get_blob_path(self.hash)
