import enum
from typing import Dict, Any, List

BackupTagDict = Dict[str, Any]


class FileRole(enum.IntEnum):
	unknown = 0
	standalone = 1
	delta_override = 2
	delta_add = 3
	delta_remove = 4

	@classmethod
	def standalone_roles(cls) -> List['FileRole']:
		return [cls.standalone]

	@classmethod
	def standalone_role_ints(cls) -> List[int]:
		return [role.value for role in cls.standalone_roles()]

	@classmethod
	def delta_roles(cls) -> List['FileRole']:
		return [
			cls.delta_override,
			cls.delta_add,
			cls.delta_remove,
		]

	@classmethod
	def delta_role_ints(cls) -> List[int]:
		return [role.value for role in cls.delta_roles()]
