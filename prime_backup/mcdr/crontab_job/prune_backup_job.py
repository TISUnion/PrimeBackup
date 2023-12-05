from typing import TYPE_CHECKING

from apscheduler.schedulers.base import BaseScheduler

from prime_backup.config.prune_config import PruneConfig
from prime_backup.mcdr.crontab_job import CrontabJob, CrontabJobId
from prime_backup.mcdr.task.backup.prune_backup_task import PruneAllBackupTask
from prime_backup.types.units import Duration

if TYPE_CHECKING:
	from prime_backup.mcdr.task_manager import TaskManager


class PruneBackupJob(CrontabJob):
	def __init__(self, scheduler: BaseScheduler, task_manager: 'TaskManager'):
		super().__init__(scheduler, task_manager)
		self.config: PruneConfig = self._root_config.prune

	@property
	def id(self) -> CrontabJobId:
		return CrontabJobId.prune_backup

	@property
	def interval(self) -> Duration:
		return self.config.interval

	@property
	def jitter(self) -> Duration:
		return self.config.jitter

	def run(self):
		self.logger.info('Prune backup job started')

		# enable state is checked inside the task
		task = PruneAllBackupTask(self.get_command_source())
		result = self.run_task_with_retry(task, self.config.interval.value > 300)
		if result.executed:
			self.logger.info('Prune backup job done')
