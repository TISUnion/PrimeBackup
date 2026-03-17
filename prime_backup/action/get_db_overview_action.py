import dataclasses

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.db.values import BlobStorageMethod


@dataclasses.dataclass(frozen=True)
class DbOverviewResult:
	db_version: int
	hash_method: str

	blob_count: int
	chunk_count: int
	chunk_group_count: int
	chunk_group_chunk_binding_count: int
	blob_chunk_group_binding_count: int

	file_object_count: int
	file_total_count: int
	fileset_count: int
	backup_count: int

	blob_stored_size_sum: int
	blob_raw_size_sum: int
	direct_blob_stored_size_sum: int
	direct_blob_raw_size_sum: int
	chunked_blob_stored_size_sum: int
	chunked_blob_raw_size_sum: int
	chunk_raw_size_sum: int
	chunk_stored_size_sum: int
	file_raw_size_sum: int

	db_file_size: int


class GetDbOverviewAction(Action[DbOverviewResult]):
	@override
	def run(self) -> DbOverviewResult:
		db_file_size = DbAccess.get_db_file_path().stat().st_size
		with DbAccess.open_session() as session:
			meta = session.get_db_meta()
			blob_size_sums = session.get_blob_size_sums_by_storage_method()
			return DbOverviewResult(
				db_version=meta.version,
				hash_method=meta.hash_method,

				blob_count=session.get_blob_count(),
				chunk_count=session.get_chunk_count(),
				chunk_group_count=session.get_chunk_group_count(),
				chunk_group_chunk_binding_count=session.get_chunk_group_chunk_binding_count(),
				blob_chunk_group_binding_count=session.get_blob_chunk_group_binding_count(),

				file_object_count=session.get_file_object_count(),
				file_total_count=session.get_file_total_count(),
				fileset_count=session.get_fileset_count(),
				backup_count=session.get_backup_count(),

				blob_stored_size_sum=sum(ss.stored_size for ss in blob_size_sums.values()),
				blob_raw_size_sum=sum(ss.raw_size for ss in blob_size_sums.values()),
				direct_blob_stored_size_sum=blob_size_sums[BlobStorageMethod.direct].stored_size,
				direct_blob_raw_size_sum=blob_size_sums[BlobStorageMethod.direct].raw_size,
				chunked_blob_stored_size_sum=blob_size_sums[BlobStorageMethod.chunked].stored_size,
				chunked_blob_raw_size_sum=blob_size_sums[BlobStorageMethod.chunked].raw_size,
				chunk_raw_size_sum=session.get_chunk_raw_size_sum(),
				chunk_stored_size_sum=session.get_chunk_stored_size_sum(),
				file_raw_size_sum=session.get_file_total_raw_size_sum(),

				db_file_size=db_file_size,
			)
