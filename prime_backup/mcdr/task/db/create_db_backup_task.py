import contextlib
import dataclasses
import datetime
import re
import tarfile
import threading
import time
from pathlib import Path
from typing import Optional, List

from typing_extensions import override

from prime_backup.action.vacuum_sqlite_action import VacuumSqliteAction
from prime_backup.db import db_constants
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.types.units import ByteCount
from prime_backup.utils import misc_utils
from prime_backup.utils.run_once import RunOnceFunc


@dataclasses.dataclass(frozen=True)
class DbBackupFile:
	path: Path
	date: datetime.datetime


class CreateDbBackupTask(HeavyTask[Optional[threading.Thread]]):
	__task_sem = threading.Semaphore(1)
	_db_backup_file_regex = re.compile(r'^db_backup_(?P<date>\d{8})_(?P<time>\d{6})\.tar\.xz$')

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
			db_backup_root.mkdir(parents=True, exist_ok=True)
			temp_db_path = db_backup_root / 'temp.db'

			self.logger.info('db backup: Vacuum database to {}'.format(temp_db_path.as_posix()))
			if temp_db_path.is_file():
				temp_db_path.unlink()

			def tar_thread():
				db_backup_file = db_backup_root / time.strftime('db_backup_%Y%m%d_%H%M%S.tar.xz')
				db_backup_file_tmp = db_backup_file.with_name(db_backup_file.name + '.tmp')
				try:
					t = time.time()

					self.logger.info('db backup: Compressing database backup {}'.format(db_backup_file.name))
					db_backup_file_tmp.unlink(missing_ok=True)
					with tarfile.open(db_backup_file_tmp, 'w:xz') as tar:
						tar.add(temp_db_path, db_constants.DB_FILE_NAME)
					db_backup_file_tmp.replace(db_backup_file)

					backup_size = db_backup_file.stat().st_size
					cost = time.time() - t
					self.logger.info('db backup: Compress database backup done, path {!r}, cost {:.2f}s, size {} ({})'.format(
						db_backup_file.as_posix(), cost, ByteCount(backup_size).auto_str(), f'{100 * backup_size / db_size:.2f}%',
					))
				except Exception:
					with contextlib.suppress(OSError):
						db_backup_file_tmp.unlink(missing_ok=True)
					self.logger.exception('db backup: Compress database backup to {} failed'.format(db_backup_file))
				else:
					try:
						self.__delete_old_db_backup_files(db_backup_root, db_backup_file)
					except Exception:
						self.logger.exception('db backup: Delete old database backups failed')
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

	@classmethod
	def __parse_db_backup_file(cls, path: Path) -> Optional[DbBackupFile]:
		if not path.is_file():
			return None
		if (match := cls._db_backup_file_regex.fullmatch(path.name)) is None:
			return None

		try:
			date = datetime.datetime.strptime(match['date'] + match['time'], '%Y%m%d%H%M%S')
		except ValueError:
			return None
		return DbBackupFile(path=path, date=date)

	@classmethod
	def _get_db_backup_files(cls, db_backup_root: Path) -> List[DbBackupFile]:
		files: List[DbBackupFile] = []
		for path in db_backup_root.iterdir():
			if (backup_file := cls.__parse_db_backup_file(path)) is not None:
				files.append(backup_file)

		def get_sort_key(f: DbBackupFile) -> tuple:
			return f.date, f.path.name

		files.sort(key=get_sort_key, reverse=True)  # new first
		return files

	def __delete_old_db_backup_files(self, db_backup_root: Path, current_backup_file: Path):
		max_amount = self.config.database.backup.max_amount
		if max_amount <= 0:
			return

		db_backup_files = self._get_db_backup_files(db_backup_root)  # [new, ..., old)
		old_db_backup_files = [backup_file for backup_file in db_backup_files if backup_file.path != current_backup_file]
		files_to_delete = old_db_backup_files[max_amount - 1:]
		if not files_to_delete:
			return

		self.logger.info('db backup: Deleting {} old database backup(s), keeping {}'.format(len(files_to_delete), max_amount))
		for backup_file in files_to_delete:
			try:
				backup_file.path.unlink()
			except Exception:
				self.logger.exception('db backup: Failed to delete old database backup {}'.format(backup_file.path.as_posix()))
			else:
				self.logger.info('db backup: Deleted old database backup {}'.format(backup_file.path.as_posix()))
