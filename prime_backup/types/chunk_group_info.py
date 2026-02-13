import dataclasses

from prime_backup.db import schema
from prime_backup.db.values import OffsetChunkGroup


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
