import dataclasses
from pathlib import Path
from typing import Iterable, TYPE_CHECKING, Union

from typing_extensions import Self

from prime_backup.compressors import CompressMethod
from prime_backup.db import schema
from prime_backup.db.values import ChunkRow, OffsetChunk, OffsetChunkRow
from prime_backup.types.pack_info import PackEntryLocation
from prime_backup.utils import misc_utils

if TYPE_CHECKING:
	from prime_backup.types.pack_info import PackChangeSummary


@dataclasses.dataclass(frozen=True)
class ChunkInfo:
	id: int
	hash: str
	compress: CompressMethod
	raw_size: int
	stored_size: int
	pack_entry: PackEntryLocation

	@classmethod
	def of(cls, chunk: Union[schema.Chunk, ChunkRow]) -> 'ChunkInfo':
		return ChunkInfo(
			id=chunk.id,
			hash=chunk.hash,
			compress=CompressMethod[chunk.compress],
			raw_size=chunk.raw_size,
			stored_size=chunk.stored_size,
			pack_entry=PackEntryLocation(chunk.pack_id, chunk.pack_offset),
		)

	@property
	def pack_file_path(self) -> Path:
		from prime_backup.utils import pack_utils
		return pack_utils.get_pack_path(self.pack_entry.pack_id)


@dataclasses.dataclass(frozen=True)
class OffsetChunkInfo:
	offset: int
	chunk: ChunkInfo

	@property
	def size(self) -> int:
		return self.chunk.raw_size

	@classmethod
	def of(cls, offset_chunk: Union[OffsetChunk, OffsetChunkRow]) -> 'OffsetChunkInfo':
		return cls(offset_chunk.offset, ChunkInfo.of(offset_chunk.chunk))

	def __lt__(self, other: 'OffsetChunkInfo') -> bool:
		return self.offset < other.offset


@dataclasses.dataclass
class ChunkListSummary:
	count: int
	raw_size: int
	stored_size: int
	packs: 'PackChangeSummary'

	@classmethod
	def zero(cls) -> Self:
		from prime_backup.types.pack_info import PackChangeSummary
		return cls(0, 0, 0, PackChangeSummary.zero())

	def add_chunk(self, raw_size: int, stored_size: int):
		self.count += 1
		self.raw_size += raw_size
		self.stored_size += stored_size

	@classmethod
	def of(cls, chunks: Iterable[ChunkInfo]) -> 'ChunkListSummary':
		cnt, raw_size_sum, stored_size_sum = 0, 0, 0
		for chunk in chunks:
			cnt += 1
			raw_size_sum += chunk.raw_size
			stored_size_sum += chunk.stored_size
		from prime_backup.types.pack_info import PackChangeSummary
		return ChunkListSummary(
			count=cnt,
			raw_size=raw_size_sum,
			stored_size=stored_size_sum,
			packs=PackChangeSummary.zero(),
		)

	def __add__(self, other: Self) -> 'ChunkListSummary':
		misc_utils.ensure_type(other, type(self))
		return ChunkListSummary(
			count=self.count + other.count,
			raw_size=self.raw_size + other.raw_size,
			stored_size=self.stored_size + other.stored_size,
			packs=self.packs + other.packs,
		)
