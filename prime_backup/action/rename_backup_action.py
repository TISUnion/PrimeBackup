from prime_backup.action import Action
from prime_backup.db.access import DbAccess


class RenameBackupAction(Action[None]):
	def __init__(self, backup_id: int, comment: str):
		super().__init__()
		self.backup_id = backup_id
		self.comment = comment

	def run(self) -> None:
		with DbAccess.open_session() as session:
			session.get_backup(self.backup_id).comment = self.comment
