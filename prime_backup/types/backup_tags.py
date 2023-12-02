import enum
from typing import Optional, TYPE_CHECKING, Any, Type

from prime_backup.utils import misc_utils

if TYPE_CHECKING:
	from prime_backup.db.schema import BackupTagDict


class BackupTagValueType:
	def __init__(self, value_type: Type):
		self.type = value_type


class BackupTagName(enum.Enum):
	# name -> type
	pre_restore_backup = BackupTagValueType(bool)
	hidden = BackupTagValueType(bool)


def __check_backup_tag_keys():
	for name in BackupTagName:
		if name.value.type not in [bool, str, int, float]:
			raise TypeError(name.value.type)


__check_backup_tag_keys()


class BackupTags:
	data: 'BackupTagDict'

	def __init__(self, data: Optional['BackupTagDict'] = None):
		self.data = {}
		if data is not None:
			self.data.update(data)

	def get(self, name: BackupTagName) -> Any:
		return self.data.get(name.name)

	def set(self, name: BackupTagName, value: Any) -> 'BackupTags':
		self.data[name.name] = misc_utils.ensure_type(value, name.value.type)
		return self

	def to_dict(self) -> 'BackupTagDict':
		return self.data.copy()

	def __len__(self) -> int:
		return len(self.data)

	def items(self):
		return self.data.items()

	# ============ accessors ============

	def is_hidden(self) -> bool:
		return self.get(BackupTagName.hidden) is True

	def is_backup_before_restore(self) -> bool:
		return self.get(BackupTagName.pre_restore_backup) is True
