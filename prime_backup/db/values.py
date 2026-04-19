import dataclasses
import enum
from typing import Dict, Any, List, TYPE_CHECKING

if TYPE_CHECKING:
	from prime_backup.db import schema

BackupTagDict = Dict[str, Any]


class BlobStorageMethod(enum.IntEnum):
	unknown = 0
	direct = 1   # at blob store (regular method)
	chunked = 2  # at chunk store (chunked into chunks)


@dataclasses.dataclass(frozen=True)
class OffsetChunk:
	offset: int
	chunk: 'schema.Chunk'

	def __lt__(self, other: 'OffsetChunk'):
		return self.offset < other.offset


@dataclasses.dataclass(frozen=True)
class OffsetChunkGroup:
	offset: int
	chunk_group: 'schema.ChunkGroup'

	def __lt__(self, other: 'OffsetChunkGroup'):
		return self.offset < other.offset


@dataclasses.dataclass(frozen=True)
class ChunkGroupChunkBindingIdentifier:
	chunk_group_id: int
	chunk_offset: int

	@classmethod
	def of(cls, binding: 'schema.ChunkGroupChunkBinding') -> 'ChunkGroupChunkBindingIdentifier':
		return ChunkGroupChunkBindingIdentifier(
			chunk_group_id=binding.chunk_group_id,
			chunk_offset=binding.chunk_offset,
		)


@dataclasses.dataclass(frozen=True)
class BlobChunkGroupBindingIdentifier:
	blob_id: int
	chunk_group_offset: int

	@classmethod
	def of(cls, binding: 'schema.BlobChunkGroupBinding') -> 'BlobChunkGroupBindingIdentifier':
		return BlobChunkGroupBindingIdentifier(
			blob_id=binding.blob_id,
			chunk_group_offset=binding.chunk_group_offset,
		)


@dataclasses.dataclass(frozen=True)
class FileIdentifier:
	fileset_id: int
	path: str

	@classmethod
	def of(cls, file: 'schema.File') -> 'FileIdentifier':
		return FileIdentifier(
			fileset_id=file.fileset_id,
			path=file.path,
		)


class FileRole(enum.IntEnum):
	unknown = 0
	standalone = 1
	delta_override = 2
	delta_add = 3
	delta_remove = 4

	@classmethod
	def standalone_roles(cls) -> List['FileRole']:
		return [cls.standalone]

	@classmethod
	def standalone_role_ints(cls) -> List[int]:
		return [role.value for role in cls.standalone_roles()]

	@classmethod
	def delta_roles(cls) -> List['FileRole']:
		return [
			cls.delta_override,
			cls.delta_add,
			cls.delta_remove,
		]

	@classmethod
	def delta_role_ints(cls) -> List[int]:
		return [role.value for role in cls.delta_roles()]
