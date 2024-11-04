from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.types.fileset_info import FilesetInfo


class GetFilesetAction(Action[FilesetInfo]):
	def __init__(self, fileset_id: int, *, count_backups: bool = False):
		super().__init__()
		self.fileset_id = fileset_id
		self.count_backups = count_backups

	def run(self) -> FilesetInfo:
		with DbAccess.open_session() as session:
			backup_count = session.get_fileset_reference_count(self.fileset_id) if self.count_backups else 0
			return FilesetInfo.of(session.get_fileset(self.fileset_id), backup_count=backup_count)
