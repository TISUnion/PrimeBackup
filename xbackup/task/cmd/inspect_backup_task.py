from mcdreforged.command.command_source import CommandSource

from xbackup.db.access import DbAccess
from xbackup.task.task import Task


class InspectBackupTask(Task):
	def __init__(self, source: CommandSource, backup_id: int):
		super().__init__()
		self.source = source
		self.backup_id = backup_id

	def run(self):
		# ensure backup exists first
		with DbAccess.open_session() as session:
			backup = session.get_backup_or_throw(self.backup_id)
