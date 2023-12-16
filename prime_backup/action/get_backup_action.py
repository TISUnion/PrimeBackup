from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.types.backup_info import BackupInfo
from prime_backup.utils import misc_utils


class GetBackupAction(Action[BackupInfo]):
	def __init__(self, backup_id: int, *, with_files: bool = False):
		super().__init__()
		self.backup_id = misc_utils.ensure_type(backup_id, int)
		self.with_files = with_files

	def run(self) -> BackupInfo:
		with DbAccess.open_session() as session:
			backup = session.get_backup(self.backup_id)
			return BackupInfo.of(backup, with_files=self.with_files)
