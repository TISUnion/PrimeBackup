from pathlib import Path
from typing import NamedTuple, Optional

from prime_backup.action import Action
from prime_backup.db.access import DbAccess


class FileSizeDiff(NamedTuple):
	before: int
	after: int

	@property
	def diff(self) -> int:
		return self.after - self.before


class VacuumSqliteAction(Action[FileSizeDiff]):
	def __init__(self, target_path: Optional[Path] = None):
		super().__init__()
		self.target_path = target_path

	def run(self) -> FileSizeDiff:
		db_path = DbAccess.get_db_path()
		prev_size = db_path.stat().st_size

		if self.target_path is not None:
			self.target_path.parent.mkdir(parents=True, exist_ok=True)
			into_file = self.target_path.as_posix()
		else:
			into_file = None

		with DbAccess.open_session() as session:
			session.vacuum(into_file)

		if self.target_path is not None:
			after_size = self.target_path.stat().st_size
		else:
			after_size = db_path.stat().st_size
		return FileSizeDiff(prev_size, after_size)
