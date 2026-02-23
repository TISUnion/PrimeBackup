import dataclasses
from typing import Optional, TYPE_CHECKING

from prime_backup.db import schema

if TYPE_CHECKING:
	from prime_backup.types.chunk_group_info import ChunkGroupInfo
	from prime_backup.types.chunk_info import ChunkInfo


@dataclasses.dataclass(frozen=True)
class ChunkGroupChunkBindingInfo:
	chunk_group_id: int
	chunk_offset: int
	chunk_id: int

	chunk_group: Optional['ChunkGroupInfo'] = None
	chunk: Optional['ChunkInfo'] = None

	@classmethod
	def of(
			cls, binding: schema.ChunkGroupChunkBinding, *,
			chunk_group: Optional['ChunkGroupInfo'] = None,
			chunk: Optional['ChunkInfo'] = None
	) -> 'ChunkGroupChunkBindingInfo':
		return cls(
			chunk_group_id=binding.chunk_group_id,
			chunk_offset=binding.chunk_offset,
			chunk_id=binding.chunk_id,
			chunk_group=chunk_group,
			chunk=chunk,
		)
