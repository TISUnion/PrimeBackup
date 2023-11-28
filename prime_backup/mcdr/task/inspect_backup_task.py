from mcdreforged.api.all import *

from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.mcdr.task import Task, TaskType


class InspectBackupTask(Task):
	def __init__(self, source: CommandSource, backup_id: int):
		super().__init__(source)
		self.backup_id = backup_id

	@property
	def name(self) -> str:
		return 'inspect'

	def type(self) -> TaskType:
		return TaskType.read

	def run(self):
		# TODO
		backup = GetBackupAction(self.backup_id).run()
