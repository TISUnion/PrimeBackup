import dataclasses
from pathlib import Path
from typing import Iterable

from typing_extensions import Self

from prime_backup.db import schema
from prime_backup.utils import misc_utils


@dataclasses.dataclass(frozen=True)
class PackInfo:
	id: int
	name: str
	size: int
	count: int
	live_size: int
	live_count: int

	@classmethod
	def of(cls, pack: schema.Pack) -> 'PackInfo':
		return PackInfo(
			id=pack.id,
			name=pack.name,
			size=pack.size,
			count=pack.count,
			live_size=pack.live_size,
			live_count=pack.live_count,
		)

	@property
	def file_path(self) -> Path:
		from prime_backup.utils import pack_utils
		return pack_utils.get_pack_path(self.name)

	@property
	def dead_size(self) -> int:
		return max(0, self.size - self.live_size)


@dataclasses.dataclass(frozen=True)
class PackEntryLocation:
	pack_id: int
	pack_name: str
	pack_offset: int


@dataclasses.dataclass(frozen=True)
class PackEntryInfo:
	id: int
	pack_id: int
	offset: int
	size: int


@dataclasses.dataclass
class PackChangeSummary:
	compacted_pack_count: int = 0
	removed_pack_count: int = 0
	old_size: int = 0
	new_size: int = 0

	@property
	def touched_pack_count(self) -> int:
		return self.compacted_pack_count + self.removed_pack_count

	@property
	def freed_size(self) -> int:
		return self.old_size - self.new_size

	@property
	def created_size(self) -> int:
		return max(0, self.new_size - self.old_size)

	@property
	def freed_size_clamped(self) -> int:
		return max(0, self.freed_size)

	@property
	def raw_size(self) -> int:
		return self.freed_size

	@property
	def stored_size(self) -> int:
		return self.freed_size

	@classmethod
	def zero(cls) -> Self:
		return cls()

	@property
	def count(self) -> int:
		return self.touched_pack_count

	@classmethod
	def of_created_packs(cls, packs: Iterable[PackInfo]) -> 'PackChangeSummary':
		summary = cls()
		for pack in packs:
			summary.new_size += pack.size
		return summary

	def __add__(self, other: Self) -> 'PackChangeSummary':
		misc_utils.ensure_type(other, type(self))
		return PackChangeSummary(
			compacted_pack_count=self.compacted_pack_count + other.compacted_pack_count,
			removed_pack_count=self.removed_pack_count + other.removed_pack_count,
			old_size=self.old_size + other.old_size,
			new_size=self.new_size + other.new_size,
		)

	def __iadd__(self, other: Self) -> 'PackChangeSummary':
		misc_utils.ensure_type(other, type(self))
		self.compacted_pack_count += other.compacted_pack_count
		self.removed_pack_count += other.removed_pack_count
		self.old_size += other.old_size
		self.new_size += other.new_size
		return self
