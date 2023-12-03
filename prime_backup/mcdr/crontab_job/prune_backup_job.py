from typing import TYPE_CHECKING

from apscheduler.schedulers.base import BaseScheduler
from apscheduler.triggers.base import BaseTrigger
from apscheduler.triggers.interval import IntervalTrigger

from prime_backup.config.sub_configs import PruneConfig
from prime_backup.mcdr import mcdr_globals
from prime_backup.mcdr.crontab_job import CrontabJob, CrontabJobId
from prime_backup.mcdr.task.prune_backup_task import PruneAllBackupTask

if TYPE_CHECKING:
	from prime_backup.mcdr.task_manager import TaskManager


class PruneBackupJob(CrontabJob):
	def __init__(self, scheduler: BaseScheduler, task_manager: 'TaskManager'):
		super().__init__(scheduler, task_manager)
		self.config: PruneConfig = self._root_config.prune

	@property
	def id(self) -> CrontabJobId:
		return CrontabJobId.prune_backup

	def _create_trigger(self) -> BaseTrigger:
		return IntervalTrigger(seconds=self.config.interval.value, jitter=self.config.jitter.value)

	def run(self):
		self.logger.info('Prune backup job started')

		source = mcdr_globals.server.get_plugin_command_source()
		task = PruneAllBackupTask(source)
		result = self.run_task_with_retry(task, self.config.interval.value > 300)
		if result.executed:
			self.logger.info('Prune backup job done')
