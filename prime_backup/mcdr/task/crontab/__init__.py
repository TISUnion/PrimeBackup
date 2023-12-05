from abc import ABC

from mcdreforged.api.all import CommandSource

from prime_backup.mcdr.crontab_job import CrontabJobId, CrontabJob
from prime_backup.mcdr.crontab_manager import CrontabManager
from prime_backup.mcdr.task.basic_tasks import ImmediateTask


class CrontabTaskBase(ImmediateTask, ABC):
	def __init__(self, source: CommandSource, crontab_manager: CrontabManager, job_id: CrontabJobId):
		super().__init__(source)
		self.crontab_manager = crontab_manager
		self.job_id = job_id

	def get_job(self) -> CrontabJob:
		return self.crontab_manager.get_job(self.job_id)
