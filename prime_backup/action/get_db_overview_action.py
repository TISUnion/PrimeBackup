import dataclasses

from prime_backup.action import Action
from prime_backup.db.access import DbAccess


@dataclasses.dataclass(frozen=True)
class DbOverviewResult:
	db_version: int
	hash_method: str

	blob_count: int
	file_count: int
	backup_count: int

	blob_stored_size_sum: int
	blob_raw_size_sum: int
	file_raw_size_sum: int

	db_file_size: int


class GetDbOverviewAction(Action[DbOverviewResult]):
	def run(self) -> DbOverviewResult:
		db_file_size = DbAccess.get_db_file_path().stat().st_size
		with DbAccess.open_session() as session:
			meta = session.get_db_meta()
			return DbOverviewResult(
				db_version=meta.version,
				hash_method=meta.hash_method,

				blob_count=session.get_blob_count(),
				file_count=session.get_file_count(),
				backup_count=session.get_backup_count(),

				blob_stored_size_sum=session.get_blob_stored_size_sum(),
				blob_raw_size_sum=session.get_blob_raw_size_sum(),
				file_raw_size_sum=session.get_file_raw_size_sum(),

				db_file_size=db_file_size,
			)
