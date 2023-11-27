from xbackup.mcdr.task import Task


class DeleteBackupTask(Task):
	def __init__(self, backup_id: int):
		super().__init__()

	def run(self):
		pass
