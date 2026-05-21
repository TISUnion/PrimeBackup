import dataclasses
from pathlib import Path

from typing_extensions import Self

from prime_backup.db import schema
from prime_backup.utils import misc_utils


@dataclasses.dataclass(frozen=True)
class PackInfo:
	id: int
	size: int
	entry_count: int
	live_size: int
	live_entry_count: int

	@classmethod
	def of(cls, pack: schema.Pack) -> 'PackInfo':
		return PackInfo(
			id=pack.id,
			size=pack.size,
			entry_count=pack.entry_count,
			live_size=pack.live_size,
			live_entry_count=pack.live_entry_count,
		)

	@property
	def file_path(self) -> Path:
		from prime_backup.utils import pack_utils
		return pack_utils.get_pack_path(self.id)

	@property
	def file_name(self) -> str:
		from prime_backup.utils import pack_utils
		return pack_utils.get_pack_file_name(self.id)


@dataclasses.dataclass(frozen=True)
class PackEntryLocation:
	pack_id: int
	offset: int


@dataclasses.dataclass(frozen=True)
class PackEntryInfo:
	pack_id: int
	offset: int
	size: int

	# entry information
	chunk_id: int


@dataclasses.dataclass
class PackChangeSummary:
	created_pack_count: int = 0
	updated_pack_count: int = 0
	compacted_pack_count: int = 0
	removed_pack_count: int = 0
	old_size: int = 0
	new_size: int = 0

	@property
	def reclaimed_pack_count(self) -> int:
		return self.compacted_pack_count + self.removed_pack_count

	@property
	def changed_pack_count(self) -> int:
		return self.created_pack_count + self.updated_pack_count + self.reclaimed_pack_count

	@property
	def freed_size(self) -> int:
		return max(0, self.old_size - self.new_size)

	@property
	def created_size(self) -> int:
		return max(0, self.new_size - self.old_size)

	@classmethod
	def zero(cls) -> Self:
		return cls()

	def __add__(self, other: Self) -> 'PackChangeSummary':
		misc_utils.ensure_type(other, type(self))
		return PackChangeSummary(
			created_pack_count=self.created_pack_count + other.created_pack_count,
			updated_pack_count=self.updated_pack_count + other.updated_pack_count,
			compacted_pack_count=self.compacted_pack_count + other.compacted_pack_count,
			removed_pack_count=self.removed_pack_count + other.removed_pack_count,
			old_size=self.old_size + other.old_size,
			new_size=self.new_size + other.new_size,
		)

	def __iadd__(self, other: Self) -> 'PackChangeSummary':
		misc_utils.ensure_type(other, type(self))
		self.created_pack_count += other.created_pack_count
		self.updated_pack_count += other.updated_pack_count
		self.compacted_pack_count += other.compacted_pack_count
		self.removed_pack_count += other.removed_pack_count
		self.old_size += other.old_size
		self.new_size += other.new_size
		return self
