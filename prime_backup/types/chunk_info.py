import dataclasses

from prime_backup.compressors import CompressMethod
from prime_backup.db import schema


@dataclasses.dataclass(frozen=True)
class ChunkInfo:
	hash: str
	compress: CompressMethod
	raw_size: int
	stored_size: int

	@classmethod
	def of(cls, chunk: schema.Chunk) -> 'ChunkInfo':
		"""
		Notes: should be inside a session
		"""
		return ChunkInfo(
			hash=chunk.hash,
			compress=CompressMethod[chunk.compress],
			raw_size=chunk.raw_size,
			stored_size=chunk.stored_size,
		)


@dataclasses.dataclass(frozen=True)
class BlobChunkBinding:
	blob_hash: str
	chunk_offset: int
	chunk_hash: str
	chunk_raw_size: int
	chunk_stored_size: int

	@classmethod
	def of(cls, bcb: schema.BlobChunkBinding) -> 'BlobChunkBinding':
		return BlobChunkBinding(
			blob_hash=bcb.blob_hash,
			chunk_offset=bcb.chunk_offset,
			chunk_hash=bcb.chunk_hash,
			chunk_raw_size=bcb.chunk_raw_size,
			chunk_stored_size=bcb.chunk_stored_size,
		)
