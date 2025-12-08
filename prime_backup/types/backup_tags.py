import enum
from typing import Optional, Any, Type, List

from mcdreforged.api.all import RText, RTextBase, RColor

from prime_backup.db.values import BackupTagDict
from prime_backup.utils import misc_utils


class BackupTagValue:
	def __init__(self, value_type: Type, char: str, color: RColor):
		self.type: Type = value_type
		self.char = char
		self.color = color

	@property
	def text(self) -> RTextBase:
		from prime_backup.utils.mcdr_utils import tr
		from prime_backup.mcdr.text_components import TextColors
		return tr(f'backup_tag.{BackupTagName(self).name}').set_color(TextColors.backup_tag)

	@property
	def flag(self) -> RTextBase:
		return RText(self.char, self.color).h(self.text)


class BackupTagName(enum.Enum):
	# name -> type
	hidden = BackupTagValue(bool, 'H', RColor.blue)
	temporary = BackupTagValue(bool, 'T', RColor.yellow)
	protected = BackupTagValue(bool, 'P', RColor.dark_green)
	scheduled = BackupTagValue(bool, 'S', RColor.dark_blue)

	@classmethod
	def bool_tags(cls) -> List['BackupTagName']:
		return [
			tag for tag in cls
			if tag.value.type is bool
		]


def __check_backup_tag_keys():
	for name in BackupTagName:
		if name.value.type not in [bool, str, int, float]:
			raise TypeError(name.value.type)


__check_backup_tag_keys()


class BackupTags:
	NONE = object()

	def __init__(self, data: Optional[BackupTagDict] = None):
		self.__data: BackupTagDict = {}
		if data is not None:
			self.__data.update(data)

	def get(self, name: BackupTagName) -> Any:
		return self.__data.get(name.name) if name.name in self.__data else self.NONE

	def set(self, name: BackupTagName, value: Any) -> 'BackupTags':
		self.__data[name.name] = misc_utils.ensure_type(value, name.value.type)
		return self

	def clear(self, name: BackupTagName) -> bool:
		try:
			self.__data.pop(name.name)
			return True
		except KeyError:
			return False

	def to_dict(self) -> 'BackupTagDict':
		return self.__data.copy()

	def __len__(self) -> int:
		return len(self.__data)

	def __repr__(self) -> str:
		return f'BackupTags({self.__data!r})'

	def __str__(self) -> str:
		return str(self.__data)

	def items(self):
		return self.__data.items()

	# ============ accessors ============

	def is_hidden(self) -> bool:
		return self.get(BackupTagName.hidden) is True

	def is_temporary_backup(self) -> bool:
		return self.get(BackupTagName.temporary) is True

	def is_scheduled_backup(self) -> bool:
		return self.get(BackupTagName.scheduled) is True

	def is_protected(self) -> bool:
		return self.get(BackupTagName.protected) is True
