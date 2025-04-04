from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.types.backup_info import BackupInfo
from prime_backup.utils import misc_utils


class GetBackupAction(Action[BackupInfo]):
	def __init__(self, backup_id: int, *, with_files: bool = False):
		super().__init__()
		self.backup_id = misc_utils.ensure_type(backup_id, int)
		self.with_files = with_files

	@override
	def run(self) -> BackupInfo:
		with DbAccess.open_session() as session:
			backup = session.get_backup(self.backup_id)
			if self.with_files:
				return BackupInfo.of(backup, backup_files=session.get_backup_files(backup))
			else:
				return BackupInfo.of(backup)
