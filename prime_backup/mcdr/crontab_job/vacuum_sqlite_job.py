from typing import TYPE_CHECKING

from apscheduler.schedulers.base import BaseScheduler

from prime_backup.config.database_config import CompactDatabaseConfig
from prime_backup.mcdr.crontab_job import CrontabJob, CrontabJobId
from prime_backup.mcdr.task.db.vacuum_sqlite_task import VacuumSqliteTask
from prime_backup.types.units import Duration

if TYPE_CHECKING:
	from prime_backup.mcdr.task_manager import TaskManager


class VacuumSqliteJob(CrontabJob):
	def __init__(self, scheduler: BaseScheduler, task_manager: 'TaskManager'):
		super().__init__(scheduler, task_manager)
		self.config: CompactDatabaseConfig = self._root_config.database.compact

	@property
	def id(self) -> CrontabJobId:
		return CrontabJobId.vacuum_sqlite

	@property
	def interval(self) -> Duration:
		return self.config.interval

	@property
	def jitter(self) -> Duration:
		return self.config.jitter

	def is_enabled(self) -> bool:
		return self.config.enabled

	def run(self):
		self.run_task_with_retry(VacuumSqliteTask(self.get_command_source()), True)
