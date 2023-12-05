from typing import NamedTuple

from prime_backup.action import Action
from prime_backup.db.access import DbAccess


class FileSizeDiff(NamedTuple):
	before: int
	after: int

	@property
	def diff(self) -> int:
		return self.after - self.before


class VacuumSqliteAction(Action):
	def run(self) -> FileSizeDiff:
		db_path = DbAccess.get_db_path()
		prev_size = db_path.stat().st_size

		with DbAccess.open_session() as session:
			session.vacuum()

		after_size = db_path.stat().st_size
		return FileSizeDiff(prev_size, after_size)
