from typing import TYPE_CHECKING

from apscheduler.schedulers.base import BaseScheduler

from prime_backup.config.config_common import CrontabJobSetting
from prime_backup.config.prune_config import PruneConfig
from prime_backup.mcdr.crontab_job import CrontabJobId
from prime_backup.mcdr.crontab_job.basic_job import BasicCrontabJob
from prime_backup.mcdr.task.backup.prune_backup_task import PruneAllBackupTask

if TYPE_CHECKING:
	from prime_backup.mcdr.task_manager import TaskManager


class PruneBackupJob(BasicCrontabJob):
	def __init__(self, scheduler: BaseScheduler, task_manager: 'TaskManager'):
		super().__init__(scheduler, task_manager)
		self.config: PruneConfig = self._root_config.prune

	@property
	def id(self) -> CrontabJobId:
		return CrontabJobId.prune_backup

	@property
	def job_config(self) -> CrontabJobSetting:
		return self.config

	def run(self):
		self.logger.info('Prune backup job started')

		# enable state is checked inside the task
		task = PruneAllBackupTask(self.get_command_source(), verbose=1)
		self.run_task_with_retry(task, True)
