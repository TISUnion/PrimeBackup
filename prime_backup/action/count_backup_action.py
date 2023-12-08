from typing import Optional

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.types.backup_filter import BackupFilter


class CountBackupAction(Action[int]):
	def __init__(self, backup_filter: Optional[BackupFilter] = None):
		super().__init__()
		self.backup_filter = backup_filter

	def run(self) -> int:
		with DbAccess.open_session() as session:
			return session.get_backup_count(self.backup_filter)
