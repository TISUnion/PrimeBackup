from typing import NamedTuple

from prime_backup.action import Action
from prime_backup.db.access import DbAccess


class ObjectCounts(NamedTuple):
	blob_count: int
	file_count: int
	backup_count: int


class GetObjectCountsAction(Action[ObjectCounts]):
	def run(self) -> ObjectCounts:
		with DbAccess.open_session() as session:
			return ObjectCounts(
				blob_count=session.get_blob_count(),
				file_count=session.get_file_count(),
				backup_count=session.get_backup_count(),
			)
