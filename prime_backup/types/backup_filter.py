import dataclasses
import enum
from typing import Optional, List, Any, NamedTuple

from prime_backup.types.backup_tags import BackupTagName
from prime_backup.types.operator import Operator


class BackupTagFilter(NamedTuple):
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
	author: Optional[Operator] = None
	timestamp_start: Optional[int] = None
	timestamp_end: Optional[int] = None
	tag_filters: List[BackupTagFilter] = dataclasses.field(default_factory=list)
