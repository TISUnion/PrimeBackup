import dataclasses

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess


@dataclasses.dataclass(frozen=True)
class ObjectCounts:
	blob_count: int
	chunk_count: int
	chunk_group_count: int
	chunk_group_chunk_binding_count: int
	blob_chunk_group_binding_count: int

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
				chunk_count=session.get_chunk_count(),
				chunk_group_count=session.get_chunk_group_count(),
				chunk_group_chunk_binding_count=session.get_chunk_group_chunk_binding_count(),
				blob_chunk_group_binding_count=session.get_blob_chunk_group_binding_count(),

				file_object_count=session.get_file_object_count(),
				file_total_count=session.get_file_total_count(),
				fileset_count=session.get_fileset_count(),
				backup_count=session.get_backup_count(),
			)
