import dataclasses
import enum
from typing import Optional, List, Any

from prime_backup.types.backup_tags import BackupTagName
from prime_backup.types.operator import Operator


@dataclasses.dataclass(frozen=True)
class BackupTagFilter:
	class Policy(enum.Enum):
		equals = enum.auto()
		not_equals = enum.auto()  # not equals, or not exists
		exists_and_not_equals = enum.auto()
		exists = enum.auto()
		not_exists = enum.auto()

	name: BackupTagName
	value: Any
	policy: Policy


@dataclasses.dataclass
class BackupFilter:
	id_start: Optional[int] = None
	id_end: Optional[int] = None
	creator: Optional[Operator] = None
	timestamp_start: Optional[int] = None
	timestamp_end: Optional[int] = None
	tag_filters: List[BackupTagFilter] = dataclasses.field(default_factory=list)

	def filter_temporary_backup(self) -> 'BackupFilter':
		self.tag_filters.append(BackupTagFilter(BackupTagName.temporary, True, BackupTagFilter.Policy.equals))
		return self

	def filter_non_temporary_backup(self) -> 'BackupFilter':
		self.tag_filters.append(BackupTagFilter(BackupTagName.temporary, True, BackupTagFilter.Policy.not_equals))
		return self

	def filter_non_hidden_backup(self) -> 'BackupFilter':
		self.tag_filters.append(BackupTagFilter(BackupTagName.hidden, True, BackupTagFilter.Policy.not_equals))
		return self

	def filter_non_protected_backup(self) -> 'BackupFilter':
		self.tag_filters.append(BackupTagFilter(BackupTagName.protected, True, BackupTagFilter.Policy.not_equals))
		return self
