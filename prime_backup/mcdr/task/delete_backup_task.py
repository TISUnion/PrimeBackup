from mcdreforged.api.all import *

from prime_backup.mcdr.task import Task


class DeleteBackupTask(Task):
	def __init__(self, source: CommandSource, backup_id: int):
		super().__init__()
		self.source = source
		self.backup_id = backup_id

	def run(self):
		pass
