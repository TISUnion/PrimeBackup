from pathlib import Path
from typing import NamedTuple, Iterable

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


class BlobListSummary(NamedTuple):
	count: int
	raw_size: int
	stored_size: int

	@classmethod
	def of(cls, blobs: Iterable[BlobInfo]) -> 'BlobListSummary':
		"""
		Notes: should be inside a session
		"""
		cnt, raw_size_sum, stored_size_sum = 0, 0, 0
		for blob in blobs:
			cnt += 1
			raw_size_sum += blob.raw_size
			stored_size_sum += blob.stored_size
		return BlobListSummary(
			count=cnt,
			raw_size=raw_size_sum,
			stored_size=stored_size_sum,
		)
