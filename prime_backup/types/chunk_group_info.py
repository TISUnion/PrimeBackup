import dataclasses

from prime_backup.db import schema


@dataclasses.dataclass(frozen=True)
class ChunkGroupInfo:
	id: int
	hash: str
	chunk_count: int
	chunk_raw_size_sum: int
	chunk_stored_size_sum: int

	@classmethod
	def of(cls, chunk_group: schema.ChunkGroup) -> 'ChunkGroupInfo':
		"""
		Notes: should be inside a session
		"""
		return ChunkGroupInfo(
			id=chunk_group.id,
			hash=chunk_group.hash,
			chunk_count=chunk_group.chunk_count,
			chunk_raw_size_sum=chunk_group.chunk_raw_size_sum,
			chunk_stored_size_sum=chunk_group.chunk_stored_size_sum,
		)
