import dataclasses
from typing import Optional, TYPE_CHECKING

from prime_backup.db import schema
from prime_backup.db.values import OffsetChunkGroup

if TYPE_CHECKING:
	from prime_backup.types.blob_info import BlobInfo
	from prime_backup.types.chunk_info import ChunkInfo


@dataclasses.dataclass(frozen=True)
class ChunkGroupInfo:
	id: int
	hash: str
	chunk_count: int
	chunk_raw_size_sum: int
	chunk_stored_size_sum: int

	@classmethod
	def of(cls, chunk_group: schema.ChunkGroup) -> 'ChunkGroupInfo':
		return ChunkGroupInfo(
			id=chunk_group.id,
			hash=chunk_group.hash,
			chunk_count=chunk_group.chunk_count,
			chunk_raw_size_sum=chunk_group.chunk_raw_size_sum,
			chunk_stored_size_sum=chunk_group.chunk_stored_size_sum,
		)


@dataclasses.dataclass(frozen=True)
class OffsetChunkGroupInfo:
	offset: int
	chunk_group: ChunkGroupInfo

	@classmethod
	def of(cls, offset_chunk_group: OffsetChunkGroup) -> 'OffsetChunkGroupInfo':
		return cls(offset_chunk_group.offset, ChunkGroupInfo.of(offset_chunk_group.chunk_group))


@dataclasses.dataclass(frozen=True)
class ChunkGroupChunkBindingInfo:
	chunk_group_id: int
	chunk_offset: int
	chunk_id: int

	chunk_group: Optional[ChunkGroupInfo] = None
	chunk: Optional['ChunkInfo'] = None

	@classmethod
	def of(
			cls, binding: schema.ChunkGroupChunkBinding, *,
			chunk_group: Optional[ChunkGroupInfo] = None,
			chunk: Optional['ChunkInfo'] = None
	) -> 'ChunkGroupChunkBindingInfo':
		return cls(
			chunk_group_id=binding.chunk_group_id,
			chunk_offset=binding.chunk_offset,
			chunk_id=binding.chunk_id,
			chunk_group=chunk_group,
			chunk=chunk,
		)


@dataclasses.dataclass(frozen=True)
class BlobChunkGroupBindingInfo:
	blob_id: int
	chunk_group_offset: int
	chunk_group_id: int

	blob: Optional['BlobInfo'] = None
	chunk_group: Optional[ChunkGroupInfo] = None

	@classmethod
	def of(
			cls, binding: schema.BlobChunkGroupBinding, *,
			blob: Optional['BlobInfo'] = None,
			chunk_group: Optional[ChunkGroupInfo] = None,
	) -> 'BlobChunkGroupBindingInfo':
		return cls(
			blob_id=binding.blob_id,
			chunk_group_offset=binding.chunk_group_offset,
			chunk_group_id=binding.chunk_group_id,
			blob=blob,
			chunk_group=chunk_group,
		)
