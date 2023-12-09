import tarfile
import threading
import time
from pathlib import Path
from typing import Optional

from prime_backup.action.vacuum_sqlite_action import VacuumSqliteAction
from prime_backup.db import db_constants
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.types.units import ByteCount
from prime_backup.utils import misc_utils


class CreateDbBackupTask(HeavyTask[None]):
	__task_sem = threading.Semaphore(1)

	@property
	def id(self) -> str:
		return 'db_backup'

	def run(self) -> Optional[threading.Thread]:
		if not self.__task_sem.acquire(blocking=False):
			self.logger.warning('Another {} is running, skipped'.format(self.__class__.__name__))
			return None

		try:
			db_backup_root: Path = self.config.storage_path / 'db_backup'
			temp_db_path = db_backup_root / 'temp.db'

			self.logger.info('db backup: Vacuum database to {}'.format(temp_db_path.as_posix()))
			if temp_db_path.is_file():
				temp_db_path.unlink()
			VacuumSqliteAction(temp_db_path).run()
			db_size = temp_db_path.stat().st_size
			self.logger.info('db backup: Vacuum database done, start a new thread for compressing')

			def tar_thread():
				try:
					t = time.time()
					db_backup_file = db_backup_root / time.strftime('db_backup_%Y%m%d_%H%M%S.tar.xz')

					self.logger.info('db backup: Compressing database backup {}'.format(db_backup_file.name))
					with tarfile.open(db_backup_file, 'w:xz') as tar:
						tar.add(temp_db_path, db_constants.DB_FILE_NAME)

					backup_size = db_backup_file.stat().st_size
					cost = time.time() - t
					self.logger.info('db backup: Compress database backup done, path {!r}, cost {:.2f}s, size {} ({})'.format(
						db_backup_file.as_posix(), cost, ByteCount(backup_size).auto_str(), f'{100 * backup_size / db_size:.2f}%',
					))
				finally:
					self.__task_sem.release()
					temp_db_path.unlink(missing_ok=True)

			thread = threading.Thread(target=tar_thread, name=misc_utils.make_thread_name('db-backup'), daemon=True)
			thread.start()
			return thread

		except Exception:
			self.__task_sem.release()
			raise
