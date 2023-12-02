from abc import ABC
from typing import Optional, List

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.types.backup_info import BackupInfo


class _ListBackupActionBase(Action, ABC):
	def __init__(self, *, backup_filter: Optional[BackupFilter] = None, limit: Optional[int] = None, offset: Optional[int] = None):
		super().__init__()
		self.backup_filter = backup_filter
		self.limit = limit
		self.offset = offset


class ListBackupAction(_ListBackupActionBase):
	def __init__(self, calc_size: bool = True, **kwargs):
		super().__init__(**kwargs)
		self.calc_size = calc_size

	def run(self) -> List[BackupInfo]:
		with DbAccess.open_session() as session:
			backups = session.list_backup(backup_filter=self.backup_filter, limit=self.limit, offset=self.offset)
			return [BackupInfo.of(backup.id, calc_size=self.calc_size) for backup in backups]


class ListBackupIdAction(_ListBackupActionBase):
	def run(self) -> List[int]:
		with DbAccess.open_session() as session:
			backups = session.list_backup(backup_filter=self.backup_filter, limit=self.limit, offset=self.offset)
			return [backup.id for backup in backups]
