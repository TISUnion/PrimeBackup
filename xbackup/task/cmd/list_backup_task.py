from typing import Optional

from xbackup.task.action.list_backup_action import ListBackupAction
from xbackup.task.task import Task
from xbackup.task.types.backup_filter import BackupFilter


class ListBackupTask(Task):
	def __init__(self, *, backup_filter: Optional[BackupFilter] = None, limit: Optional[int] = None):
		super().__init__()
		self.backup_filter = backup_filter
		self.limit = limit

	def run(self):
		# TODO
		ListBackupAction(self.backup_filter, self.limit).run()
