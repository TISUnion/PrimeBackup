import dataclasses
from pathlib import Path
from typing import Iterable

from typing_extensions import Self

from prime_backup.compressors import CompressMethod
from prime_backup.db import schema
from prime_backup.db.values import OffsetChunk
from prime_backup.utils import misc_utils


@dataclasses.dataclass(frozen=True)
class ChunkInfo:
	id: int
	hash: str
	compress: CompressMethod
	raw_size: int
	stored_size: int

	@classmethod
	def of(cls, chunk: schema.Chunk) -> 'ChunkInfo':
		return ChunkInfo(
			id=chunk.id,
			hash=chunk.hash,
			compress=CompressMethod[chunk.compress],
			raw_size=chunk.raw_size,
			stored_size=chunk.stored_size,
		)

	@property
	def chunk_file_path(self) -> Path:
		from prime_backup.utils import chunk_utils
		return chunk_utils.get_chunk_path(self.hash)


@dataclasses.dataclass(frozen=True)
class OffsetChunkInfo:
	offset: int
	chunk: ChunkInfo

	@classmethod
	def of(cls, offset_chunk: OffsetChunk) -> 'OffsetChunkInfo':
		return cls(offset_chunk.offset, ChunkInfo.of(offset_chunk.chunk))


@dataclasses.dataclass
class ChunkListSummary:
	count: int
	raw_size: int
	stored_size: int

	@classmethod
	def zero(cls) -> Self:
		return cls(0, 0, 0)

	@classmethod
	def of(cls, chunks: Iterable[ChunkInfo]) -> Self:
		cnt, raw_size_sum, stored_size_sum = 0, 0, 0
		for chunk in chunks:
			cnt += 1
			raw_size_sum += chunk.raw_size
			stored_size_sum += chunk.stored_size
		return ChunkListSummary(
			count=cnt,
			raw_size=raw_size_sum,
			stored_size=stored_size_sum,
		)

	def __add__(self, other: Self) -> Self:
		misc_utils.ensure_type(other, type(self))
		return ChunkListSummary(
			count=self.count + other.count,
			raw_size=self.raw_size + other.raw_size,
			stored_size=self.stored_size + other.stored_size,
		)
