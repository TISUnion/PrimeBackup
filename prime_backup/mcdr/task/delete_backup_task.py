from mcdreforged.api.all import *

from prime_backup.mcdr.task import Task, TaskType


class DeleteBackupTask(Task):
	def __init__(self, source: CommandSource, backup_id: int):
		super().__init__(source)
		self.backup_id = backup_id

	@property
	def name(self) -> str:
		return 'delete'

	def type(self) -> TaskType:
		return TaskType.operate

	def run(self):
		pass
