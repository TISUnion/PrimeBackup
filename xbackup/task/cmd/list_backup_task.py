from typing import Optional, List

from xbackup.db.access import DbAccess
from xbackup.task.task import Task
from xbackup.task.types.backup_filter import BackupFilter
from xbackup.task.types.backup_info import BackupInfo


class ListBackupTask(Task):
	def __init__(self, *, backup_filter: Optional[BackupFilter] = None, limit: int = 10):
		super().__init__()
		self.backup_filter = backup_filter
		self.limit: Optional[int] = limit if limit > 0 else None

	def run(self) -> List[BackupInfo]:
		# ensure backup exists first
		with DbAccess.open_session() as session:
			backups = session.list_backup(backup_filter=self.backup_filter, limit=self.limit)
			return list(map(BackupInfo.of, backups))
