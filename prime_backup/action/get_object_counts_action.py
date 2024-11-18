import dataclasses

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess


@dataclasses.dataclass(frozen=True)
class ObjectCounts:
	blob_count: int
	file_object_count: int
	file_total_count: int
	fileset_count: int
	backup_count: int


class GetObjectCountsAction(Action[ObjectCounts]):
	@override
	def run(self) -> ObjectCounts:
		with DbAccess.open_session() as session:
			return ObjectCounts(
				blob_count=session.get_blob_count(),
				file_object_count=session.get_file_object_count(),
				file_total_count=session.get_file_total_count(),
				fileset_count=session.get_fileset_count(),
				backup_count=session.get_backup_count(),
			)
