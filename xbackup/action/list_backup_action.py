from typing import Optional, List

from xbackup.action import Action
from xbackup.db.access import DbAccess
from xbackup.types.backup_filter import BackupFilter
from xbackup.types.backup_info import BackupInfo


class ListBackupAction(Action):
	def __init__(self, backup_filter: Optional[BackupFilter] = None, limit: Optional[int] = None):
		super().__init__()
		self.backup_filter = backup_filter
		self.limit = limit

	def run(self) -> List[BackupInfo]:
		with DbAccess.open_session() as session:
			backups = session.list_backup(backup_filter=self.backup_filter, limit=self.limit)
			return list(map(BackupInfo.of, backups))
