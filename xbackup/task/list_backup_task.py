from typing import Optional, List, NamedTuple

from xbackup.db import schema
from xbackup.db.access import DbAccess
from xbackup.task.task import Task
from xbackup.types import BackupFilter, Operator
from xbackup.utils import conversion_utils


class BackupInfo(NamedTuple):
	id: int
	timestamp_ns: int
	date: str
	author: Operator
	comment: str
	size: int  # actual uncompressed size

	@classmethod
	def of(cls, backup: schema.Backup) -> 'BackupInfo':
		"""
		Notes: should be inside a session
		"""
		size_sum = 0
		for file in backup.files:
			if file.blob_size is not None:
				size_sum += file.blob_size
		return BackupInfo(
			id=backup.id,
			timestamp_ns=backup.timestamp,
			date=conversion_utils.timestamp_to_local_date(backup.timestamp, decimal=False),
			author=Operator.of(backup.author),
			comment=backup.comment,
			size=size_sum,
		)


class ListBackupTask(Task):
	def __init__(self, *, backup_filter: Optional[BackupFilter] = None, limit: int = 10):
		super().__init__()
		self.backup_filter = backup_filter
		self.limit: Optional[int] = limit if limit > 0 else None

	def run(self) -> List[BackupInfo]:
		# ensure backup exists first
		with DbAccess.open_session() as session:
			backups = session.list_backup(backup_filter=self.backup_filter, limit=self.limit)
			return list(map(BackupInfo.of, backups))
