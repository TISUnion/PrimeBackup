import time
from typing import List, TYPE_CHECKING, Type, Any, Dict

from mcdreforged.api.all import Serializable
from typing_extensions import Self

from prime_backup.types.operator import Operator

if TYPE_CHECKING:
	from prime_backup.db import schema


class BackupMeta(Serializable):
	author: str = str(Operator.pb('import'))
	comment: str = ''
	timestamp_ns: int
	targets: List[str] = []
	tags: Dict[str, Any] = {}

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
			tags=dict(backup.tags),
		)

	def to_backup_kwargs(self) -> dict:
		return dict(
			author=self.author,
			comment=self.comment,
			timestamp=self.timestamp_ns,
			targets=[t.rstrip('/') for t in self.targets],
			tags=self.tags.copy(),
		)
