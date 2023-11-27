from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.types.backup_info import BackupInfo


class GetBackupAction(Action):
	def __init__(self, backup_id: int):
		super().__init__()
		self.backup_id = backup_id

	def run(self) -> BackupInfo:
		with DbAccess.open_session() as session:
			backup = session.get_backup_or_throw(self.backup_id)
			return BackupInfo.of(backup)
