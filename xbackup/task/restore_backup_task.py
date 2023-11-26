from xbackup.config.config import Config
from xbackup.db.access import DbAccess
from xbackup.task.create_backup_task import CreateBackupTask
from xbackup.task.export_backup_task import ExportBackupTasks
from xbackup.task.task import Task
from xbackup.types import Operator


class RestoreBackupTask(Task):
	def __init__(self, operator: Operator, backup_id: int):
		super().__init__()
		self.operator = operator
		self.backup_id = backup_id

	def run(self):
		# ensure backup exists first
		with DbAccess.open_session() as session:
			session.get_backup_or_throw(self.backup_id)

		if Config.get().backup.backup_on_overwrite:
			self.logger.info('Creating backup of existing files to avoid idiot')
			cbt = CreateBackupTask(Operator.xbackup('pre_restore'), 'Automatic backup before restoring to backup {}, executed by {}'.format(self.backup_id, self.operator))
			cbt.run()

		self.logger.info('Restoring backup')
		ExportBackupTasks.to_dir(self.backup_id, Config.get().source_path, delete_existing=True).run()
