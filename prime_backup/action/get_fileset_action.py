from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.types.fileset_info import FilesetInfo


class GetFilesetAction(Action[FilesetInfo]):
	def __init__(self, fileset_id: int, *, count_backups: bool = False, sample_backup_limit: int = 0):
		super().__init__()
		self.fileset_id = fileset_id
		self.count_backups = count_backups
		self.sample_backup_limit = sample_backup_limit

	def run(self) -> FilesetInfo:
		with DbAccess.open_session() as session:
			backup_count = session.get_fileset_associated_backup_count(self.fileset_id) if self.count_backups else 0
			sampled_backup_ids = session.get_fileset_associated_backup_ids(self.fileset_id, self.sample_backup_limit)
			return FilesetInfo.of(session.get_fileset(self.fileset_id), backup_count=backup_count, sampled_backup_ids=sampled_backup_ids)
