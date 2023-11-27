from xbackup.db.access import DbAccess
from xbackup.task.task import Task
from xbackup.task.types.backup_info import BackupInfo


class GetBackupTask(Task):
	def __init__(self, backup_id: int):
		super().__init__()
		self.backup_id = backup_id

	def run(self) -> BackupInfo:
		with DbAccess.open_session() as session:
			backup = session.get_backup_or_throw(self.backup_id)
			return BackupInfo.of(backup)
