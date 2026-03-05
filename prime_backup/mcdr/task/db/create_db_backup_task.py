import contextlib
import tarfile
import threading
import time
from pathlib import Path
from typing import Optional

from typing_extensions import override

from prime_backup.action.vacuum_sqlite_action import VacuumSqliteAction
from prime_backup.db import db_constants
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.types.units import ByteCount
from prime_backup.utils import misc_utils
from prime_backup.utils.run_once import RunOnceFunc


class CreateDbBackupTask(HeavyTask[Optional[threading.Thread]]):
	__task_sem = threading.Semaphore(1)

	@property
	@override
	def id(self) -> str:
		return 'db_backup'

	@override
	def run(self) -> Optional[threading.Thread]:
		if not self.__task_sem.acquire(blocking=False):
			self.logger.warning('Another {} is running, skipped'.format(self.__class__.__name__))
			return None

		sem_releaser = RunOnceFunc(self.__task_sem.release)

		try:
			db_backup_root: Path = self.config.storage_path / 'db_backup'
			temp_db_path = db_backup_root / 'temp.db'

			self.logger.info('db backup: Vacuum database to {}'.format(temp_db_path.as_posix()))
			if temp_db_path.is_file():
				temp_db_path.unlink()

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
					sem_releaser()
					temp_db_path.unlink(missing_ok=True)

			try:
				VacuumSqliteAction(temp_db_path).run()
				db_size = temp_db_path.stat().st_size
				self.logger.info('db backup: Vacuum database done, start a new thread for compressing')

				thread = threading.Thread(target=tar_thread, name=misc_utils.make_thread_name('db-backup'), daemon=True)
				thread.start()
				return thread

			except Exception:
				with contextlib.suppress(OSError):
					temp_db_path.unlink(missing_ok=True)
				raise

		except Exception:
			sem_releaser()
			raise
