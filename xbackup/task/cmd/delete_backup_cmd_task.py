from xbackup.task.task import Task


class DeleteBackupCommandTask(Task):
	def __init__(self, backup_id: int):
		super().__init__()

	def run(self):
		pass
