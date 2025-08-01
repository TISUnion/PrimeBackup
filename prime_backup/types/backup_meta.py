import time
from typing import List, TYPE_CHECKING, Any, Dict

from pydantic import BaseModel, Field

from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.operator import Operator

if TYPE_CHECKING:
	from prime_backup.db.session import DbSession


class BackupMeta(BaseModel):
	creator: str = str(Operator.unknown())
	comment: str = ''
	timestamp_ns: int = Field(default_factory=time.time_ns)
	targets: List[str] = Field(default_factory=list)
	tags: Dict[str, Any] = Field(default_factory=dict)

	def to_dict(self) -> dict:
		return {
			'_version': 1,
			**self.model_dump(),
		}

	@classmethod
	def from_dict(cls, dt: dict) -> 'BackupMeta':
		version = dt.get('_version', 0)

		# fix https://github.com/TISUnion/PrimeBackup/issues/87
		# where timestamp_ns was stored as float, and creator was stored as serialized Operator
		if isinstance(dt.get('timestamp_ns', None), float):
			dt['timestamp_ns'] = int(dt['timestamp_ns'])
		if isinstance(dt.get('creator', None), dict):
			dt['creator'] = str(Operator(type=dt['creator']['type'], name=dt['creator']['name']))

		return cls.model_validate(dt)

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
