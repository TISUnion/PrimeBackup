import time
from typing import List, TYPE_CHECKING, Type, Any, Dict

from mcdreforged.api.all import Serializable
from typing_extensions import Self, override

from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.operator import Operator

if TYPE_CHECKING:
	from prime_backup.db.session import DbSession


class BackupMeta(Serializable):
	creator: str = str(Operator.unknown())
	comment: str = ''
	timestamp_ns: int
	targets: List[str] = []
	tags: Dict[str, Any] = {}

	def to_dict(self) -> dict:
		return {
			'_version': 1,
			**self.serialize(),
		}

	@classmethod
	def from_dict(cls, dt: dict) -> 'BackupMeta':
		version = dt.get('_version', 0)
		if 'timestamp_ns' not in dt:
			dt['timestamp_ns'] = time.time_ns()

		# fix https://github.com/TISUnion/PrimeBackup/issues/87
		# where timestamp_ns was stored as float, and creator was stored as serialized Operator
		if isinstance(dt['timestamp_ns'], float):
			dt['timestamp_ns'] = int(dt['timestamp_ns'])
		if isinstance(dt.get('creator', None), dict):
			dt['creator'] = str(Operator(type=dt['creator']['type'], name=dt['creator']['name']))

		return cls.deserialize(dt)

	@classmethod
	@override
	def get_default(cls: Type[Self]) -> Self:
		obj = super().get_default()
		obj.timestamp_ns = time.time_ns()
		return obj

	@classmethod
	def from_backup(cls, backup: BackupInfo) -> 'BackupMeta':
		return cls(
			creator=str(backup.creator),
			comment=backup.comment,
			timestamp_ns=backup.timestamp_us * 1000,
			targets=list(backup.targets),
			tags=backup.tags.to_dict(),
		)

	def to_backup_kwargs(self) -> 'DbSession.CreateBackupKwargs':
		return dict(
			creator=self.creator,
			comment=self.comment,
			timestamp=self.timestamp_ns // 1000,
			targets=[t.rstrip('/') for t in self.targets],
			tags=self.tags.copy(),
		)
