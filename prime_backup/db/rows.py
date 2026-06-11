import dataclasses
from typing import get_args, get_type_hints

from prime_backup.db import schema


@dataclasses.dataclass(frozen=True)
class ChunkRow:
	id: int
	hash: str
	compress: str
	raw_size: int
	stored_size: int
	pack_id: int
	pack_offset: int


@dataclasses.dataclass(frozen=True)
class OffsetChunkRow:
	offset: int
	chunk: ChunkRow

	def __lt__(self, other: 'OffsetChunkRow'):
		return self.offset < other.offset


def __validate_chunk_row_fields():
	chunk_row_type_hints = get_type_hints(ChunkRow)
	chunk_row_fields = [
		(field.name, chunk_row_type_hints[field.name])
		for field in dataclasses.fields(ChunkRow)
	]

	chunk_schema_type_hints = get_type_hints(schema.Chunk)
	chunk_schema_fields = [
		(column.name, get_args(chunk_schema_type_hints[column.name])[0])
		for column in schema.Chunk.__table__.columns
	]

	if chunk_row_fields != chunk_schema_fields:
		print(chunk_row_fields)
		print(chunk_schema_fields)
		raise AssertionError('ChunkRow fields must match schema.Chunk columns')


__validate_chunk_row_fields()