from pathlib import Path
from typing import Optional

import sqlalchemy.exc
from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.types.size_diff import SizeDiff


class VacuumSqliteAction(Action[SizeDiff]):
	def __init__(self, target_path: Optional[Path] = None):
		super().__init__()
		self.target_path = target_path

	@override
	def run(self) -> SizeDiff:
		db_file_path = DbAccess.get_db_file_path()
		prev_size = db_file_path.stat().st_size

		if self.target_path is not None:
			self.target_path.parent.mkdir(parents=True, exist_ok=True)
			into_file = self.target_path.as_posix()
		else:
			into_file = None

		with DbAccess.open_session() as session:
			try:
				session.vacuum(into_file)
			except sqlalchemy.exc.OperationalError as e:
				import sqlite3
				if isinstance(e.orig, sqlite3.OperationalError) and str(e.orig) == 'database or disk is full':
					# https://github.com/TISUnion/PrimeBackup/issues/72
					self.logger.error('VACUUM failed due to {}'.format(e))
					self.logger.error('See https://github.com/TISUnion/PrimeBackup/issues/72 for more information')
				raise

		if self.target_path is not None:
			after_size = self.target_path.stat().st_size
		else:
			after_size = db_file_path.stat().st_size
		return SizeDiff(prev_size, after_size)
