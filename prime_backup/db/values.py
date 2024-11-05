import enum
from typing import Dict, Any


BackupTagDict = Dict[str, Any]


class FileRole(enum.IntEnum):
	unknown = 0
	standalone = 1
	delta_override = 2
	delta_add = 3
	delta_remove = 4
