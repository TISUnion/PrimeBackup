from typing import TYPE_CHECKING

from apscheduler.triggers.base import BaseTrigger
from apscheduler.triggers.interval import IntervalTrigger

from prime_backup.mcdr import mcdr_globals
from prime_backup.mcdr.crontab_job import CrontabJob, CrontabJobId
from prime_backup.mcdr.task.prune_backup_task import PruneAllBackupTask

if TYPE_CHECKING:
	pass


class PruneBackupJob(CrontabJob):
	@property
	def id(self) -> CrontabJobId:
		return CrontabJobId.prune_backup

	def _create_trigger(self) -> BaseTrigger:
		return IntervalTrigger(seconds=self._root_config.prune.interval.value, jitter=20)

	def run(self):
		self.logger.info('Prune backup job started')

		source = mcdr_globals.server.get_plugin_command_source()
		task = PruneAllBackupTask(source)
		self.run_task_with_retry(task, False)

		self.logger.info('Prune backup job done')
