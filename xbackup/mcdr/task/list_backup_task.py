import copy
from typing import Optional

from xbackup.action.list_backup_action import ListBackupAction
from xbackup.mcdr.task import Task
from xbackup.types.backup_filter import BackupFilter


class ListBackupTask(Task):
	def __init__(self, *, backup_filter: Optional[BackupFilter] = None, limit: Optional[int] = None, show_hidden: bool = False):
		super().__init__()
		self.backup_filter = copy.copy(backup_filter)
		self.limit = limit

		if not show_hidden:
			self.backup_filter.hidden = False

	def run(self):
		# TODO
		ListBackupAction(self.backup_filter, self.limit).run()
