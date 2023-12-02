from apscheduler.triggers.base import BaseTrigger
from apscheduler.triggers.interval import IntervalTrigger

from prime_backup.mcdr.crontab_job import CrontabJob, CrontabJobId


class PruneBackupJob(CrontabJob):
	@property
	def id(self) -> CrontabJobId:
		return CrontabJobId.prune_backup

	def _create_trigger(self) -> BaseTrigger:
		return IntervalTrigger(hours=1, jitter=10)

	def _run(self):
		pass
