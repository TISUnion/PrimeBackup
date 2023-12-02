from typing import TYPE_CHECKING

from apscheduler.schedulers.base import BaseScheduler
from apscheduler.triggers.base import BaseTrigger
from apscheduler.triggers.interval import IntervalTrigger

from prime_backup.config.sub_configs import PruneConfig
from prime_backup.mcdr.crontab_job import CrontabJob, CrontabJobId

if TYPE_CHECKING:
	from prime_backup.mcdr.task_manager import TaskManager


class PruneBackupJob(CrontabJob):
	def __init__(self, scheduler: BaseScheduler, task_manager: 'TaskManager'):
		super().__init__(scheduler, task_manager)
		self.config: PruneConfig = self.config.prune

	@property
	def id(self) -> CrontabJobId:
		return CrontabJobId.prune_backup

	def _create_trigger(self) -> BaseTrigger:
		return IntervalTrigger(hours=1, jitter=10)

	def _run(self):
		pass
