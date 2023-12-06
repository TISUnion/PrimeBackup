from typing import NamedTuple

from prime_backup.action import Action
from prime_backup.action.get_db_meta_action import GetDbMetaAction
from prime_backup.db.access import DbAccess


class GetDbOverviewAction(Action):
	class Result(NamedTuple):
		db_version: int
		hash_method: str

		blob_cnt: int
		file_cnt: int
		backup_cnt: int

		blob_stored_size_sum: int
		blob_raw_size_sum: int
		file_raw_size_sum: int

	def run(self) -> Result:
		meta = GetDbMetaAction().run()
		with DbAccess.open_session() as session:
			return self.Result(
				db_version=meta.version,
				hash_method=meta.hash_method,

				blob_cnt=session.get_blob_count(),
				file_cnt=session.get_file_count(),
				backup_cnt=session.get_backup_count(),

				blob_stored_size_sum=session.get_blob_stored_size_sum(),
				blob_raw_size_sum=session.get_blob_raw_size_sum(),
				file_raw_size_sum=session.get_file_raw_size_sum(),
			)
