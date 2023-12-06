import threading
from typing import TYPE_CHECKING

from apscheduler.schedulers.base import BaseScheduler

from prime_backup.config.database_config import BackUpDatabaseConfig
from prime_backup.mcdr.crontab_job import CrontabJobId
from prime_backup.mcdr.crontab_job.basic_job import BasicCrontabJob
from prime_backup.mcdr.task.db.create_db_backup_task import CreateDbBackupTask
from prime_backup.types.units import Duration
from prime_backup.utils import misc_utils

if TYPE_CHECKING:
	from prime_backup.mcdr.task_manager import TaskManager


class CreateDbBackupJob(BasicCrontabJob):
	def __init__(self, scheduler: BaseScheduler, task_manager: 'TaskManager'):
		super().__init__(scheduler, task_manager)
		self.config: BackUpDatabaseConfig = self._root_config.database.backup

	def is_enabled(self) -> bool:
		return self.config.enabled

	@property
	def id(self) -> CrontabJobId:
		return CrontabJobId.create_db_backup

	@property
	def interval(self) -> Duration:
		return self.config.interval

	@property
	def jitter(self) -> Duration:
		return self.config.jitter

	def run(self):
		result = self.run_task_with_retry(CreateDbBackupTask(self.get_command_source()), True, report_success=False)
		if result.ret is not None:
			misc_utils.ensure_type(result.ret, threading.Thread)
			result.ret.join()
