import dataclasses
from pathlib import Path
from typing import Iterable

from prime_backup.compressors import CompressMethod
from prime_backup.db import schema
from prime_backup.utils import misc_utils


@dataclasses.dataclass(frozen=True)
class BlobInfo:
	hash: str
	compress: CompressMethod
	raw_size: int
	stored_size: int

	file_count: int = 0

	@classmethod
	def of(cls, blob: schema.Blob, *, file_count: int = 0) -> 'BlobInfo':
		"""
		Notes: should be inside a session
		"""
		return BlobInfo(
			hash=blob.hash,
			compress=CompressMethod[blob.compress],
			raw_size=blob.raw_size,
			stored_size=blob.stored_size,
			file_count=file_count,
		)

	@property
	def blob_path(self) -> Path:
		from prime_backup.utils import blob_utils
		return blob_utils.get_blob_path(self.hash)

	def __lt__(self, other: 'BlobInfo') -> bool:
		return self.hash < other.hash


@dataclasses.dataclass(frozen=True)
class BlobListSummary:
	count: int
	raw_size: int
	stored_size: int

	@classmethod
	def zero(cls) -> 'BlobListSummary':
		return BlobListSummary(0, 0, 0)

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

	def __add__(self, other: 'BlobListSummary') -> 'BlobListSummary':
		misc_utils.ensure_type(other, type(self))
		return BlobListSummary(
			count=self.count + other.count,
			raw_size=self.raw_size + other.raw_size,
			stored_size=self.stored_size + other.stored_size,
		)
