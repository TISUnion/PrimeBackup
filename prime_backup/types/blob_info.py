import dataclasses
from pathlib import Path
from typing import Iterable, List, TYPE_CHECKING, Optional, Sequence, Union

from typing_extensions import Self

from prime_backup.compressors import CompressMethod
from prime_backup.db import schema
from prime_backup.db.values import BlobStorageMethod
from prime_backup.utils import misc_utils

if TYPE_CHECKING:
	from prime_backup.types.chunk_info import ChunkListSummary, ChunkInfo
	from prime_backup.types.file_info import FileInfo


@dataclasses.dataclass(frozen=True)
class BlobInfo:
	id: int
	storage_method: BlobStorageMethod

	hash: str
	compress: CompressMethod
	raw_size: int
	stored_size: int

	file_count: int = 0
	file_samples: List['FileInfo'] = dataclasses.field(default_factory=list)

	@classmethod
	def of(cls, blob: schema.Blob, *, file_count: int = 0, file_samples: Optional[List[schema.File]] = None) -> 'BlobInfo':
		"""
		Notes: should be inside a session
		"""
		try:
			storage_method = BlobStorageMethod(blob.storage_method)
		except (KeyError, ValueError):
			storage_method = BlobStorageMethod.unknown

		from prime_backup.types.file_info import FileInfo
		return BlobInfo(
			id=blob.id,
			storage_method=storage_method,
			hash=blob.hash,
			compress=CompressMethod[blob.compress],
			raw_size=blob.raw_size,
			stored_size=blob.stored_size,
			file_count=file_count,
			file_samples=[FileInfo.of(file) for file in file_samples] if file_samples is not None else [],
		)

	@property
	def blob_file_path(self) -> Path:
		from prime_backup.utils import blob_utils
		return blob_utils.get_blob_path(self.hash)

	def __lt__(self, other: 'BlobInfo') -> bool:
		return self.hash < other.hash


@dataclasses.dataclass
class BlobListSummary:
	count: int
	raw_size: int
	stored_size: int

	@classmethod
	def zero(cls) -> Self:
		return cls(0, 0, 0)

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

	def __add__(self, other: Self) -> 'BlobListSummary':
		misc_utils.ensure_type(other, type(self))
		return BlobListSummary(
			count=self.count + other.count,
			raw_size=self.raw_size + other.raw_size,
			stored_size=self.stored_size + other.stored_size,
		)


@dataclasses.dataclass
class BlobDeltaSummary:
	direct_blobs: BlobListSummary
	chunked_blobs: BlobListSummary
	chunks: 'ChunkListSummary'

	@classmethod
	def zero(cls) -> 'BlobDeltaSummary':
		from prime_backup.types.chunk_info import ChunkListSummary
		return BlobDeltaSummary(
			chunked_blobs=BlobListSummary.zero(),
			direct_blobs=BlobListSummary.zero(),
			chunks=ChunkListSummary.zero(),
		)

	@classmethod
	def of(cls, new_blobs: Sequence[BlobInfo], new_chunks: Union[Sequence['ChunkInfo'], 'ChunkListSummary']) -> 'BlobDeltaSummary':
		from prime_backup.types.chunk_info import ChunkListSummary
		return BlobDeltaSummary(
			direct_blobs=BlobListSummary.of(blob for blob in new_blobs if blob.storage_method == BlobStorageMethod.direct),
			chunked_blobs=BlobListSummary.of(blob for blob in new_blobs if blob.storage_method == BlobStorageMethod.chunked),
			chunks=new_chunks if isinstance(new_chunks, ChunkListSummary) else ChunkListSummary.of(new_chunks),
		)

	@property
	def blobs(self) -> BlobListSummary:
		return self.direct_blobs + self.chunked_blobs

	@property
	def blob_count(self) -> int:
		return self.direct_blobs.count + self.chunked_blobs.count

	@property
	def chunk_count(self) -> int:
		return self.chunks.count

	@property
	def raw_size(self) -> int:
		return self.direct_blobs.raw_size + self.chunks.raw_size

	@property
	def stored_size(self) -> int:
		return self.direct_blobs.stored_size + self.chunks.stored_size

	def __add__(self, other: 'BlobDeltaSummary') -> 'BlobDeltaSummary':
		misc_utils.ensure_type(other, type(self))
		return BlobDeltaSummary(
			direct_blobs=self.direct_blobs + other.direct_blobs,
			chunked_blobs=self.chunked_blobs + other.chunked_blobs,
			chunks=self.chunks + other.chunks,
		)
