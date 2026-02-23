import dataclasses
from typing import Optional, TYPE_CHECKING

from prime_backup.db import schema

if TYPE_CHECKING:
	from prime_backup.types.blob_info import BlobInfo
	from prime_backup.types.chunk_group_info import ChunkGroupInfo


@dataclasses.dataclass(frozen=True)
class BlobChunkGroupBindingInfo:
	blob_id: int
	chunk_group_offset: int
	chunk_group_id: int

	blob: Optional['BlobInfo'] = None
	chunk_group: Optional['ChunkGroupInfo'] = None

	@classmethod
	def of(
			cls, binding: schema.BlobChunkGroupBinding, *,
			blob: Optional['BlobInfo'] = None,
			chunk_group: Optional['ChunkGroupInfo'] = None,
	) -> 'BlobChunkGroupBindingInfo':
		return cls(
			blob_id=binding.blob_id,
			chunk_group_offset=binding.chunk_group_offset,
			chunk_group_id=binding.chunk_group_id,
			blob=blob,
			chunk_group=chunk_group,
		)
