from mcdreforged.command.command_source import CommandSource

from xbackup.action.get_backup_action import GetBackupAction
from xbackup.mcdr.task import Task


class InspectBackupTask(Task):
	def __init__(self, source: CommandSource, backup_id: int):
		super().__init__()
		self.source = source
		self.backup_id = backup_id

	def run(self):
		# TODO
		backup = GetBackupAction(self.backup_id).run()
