import time
from typing import List, TYPE_CHECKING, Type

from mcdreforged.utils.serializer import Serializable
from typing_extensions import Self

from prime_backup import constants
from prime_backup.types.operator import Operator

if TYPE_CHECKING:
	from prime_backup.db import schema


class BackupMeta(Serializable):
	author: str = Operator.pb('import')
	comment: str = ''
	timestamp_ns: int
	targets: List[str] = []
	hidden: bool = False

	def to_dict(self) -> dict:
		dt = self.serialize()
		dt['_version'] = 1
		return dt

	@classmethod
	def from_dict(cls, dt: dict) -> 'BackupMeta':
		version = dt.get('_version', 0)
		if 'timestamp_ns' not in dt:
			dt['timestamp_ns'] = time.time_ns()
		return cls.deserialize(dt)

	@classmethod
	def get_default(cls: Type[Self]) -> Self:
		obj = super().get_default()
		obj.timestamp_ns = time.time_ns()
		return obj

	@classmethod
	def from_backup(cls, backup: 'schema.Backup') -> 'BackupMeta':
		return cls(
			author=backup.author,
			comment=backup.comment,
			timestamp_ns=backup.timestamp,
			targets=list(backup.targets),
			hidden=backup.hidden,
		)

	@classmethod
	def get_file_name(cls) -> str:
		return constants.BACKUP_META_FILE_NAME
