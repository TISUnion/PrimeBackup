from typing import TYPE_CHECKING

from apscheduler.schedulers.base import BaseScheduler
from typing_extensions import override

from prime_backup.config.config_common import CrontabJobSetting
from prime_backup.config.database_config import CompactPackDatabaseConfig
from prime_backup.mcdr.crontab_job import CrontabJobId
from prime_backup.mcdr.crontab_job.basic_job import BasicCrontabJob
from prime_backup.mcdr.task.db.compact_packs_task import CompactPacksTask

if TYPE_CHECKING:
	from prime_backup.mcdr.task_manager import TaskManager


class CompactPackJob(BasicCrontabJob):
	def __init__(self, scheduler: BaseScheduler, task_manager: 'TaskManager'):
		super().__init__(scheduler, task_manager)
		self.config: CompactPackDatabaseConfig = self._root_config.database.compact_pack

	@property
	@override
	def id(self) -> CrontabJobId:
		return CrontabJobId.compact_pack

	@property
	@override
	def job_config(self) -> CrontabJobSetting:
		return self.config

	@override
	def run(self):
		task = CompactPacksTask(self.get_command_source(), threshold=self.config.compact_threshold)
		self.run_task_with_retry(task, True).report()
