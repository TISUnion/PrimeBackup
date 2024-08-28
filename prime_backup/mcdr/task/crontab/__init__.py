from abc import ABC
from typing import TypeVar

from mcdreforged.api.all import CommandSource

from prime_backup.mcdr.crontab_job import CrontabJobId, CrontabJob
from prime_backup.mcdr.crontab_manager import CrontabManager
from prime_backup.mcdr.task.basic_task import ImmediateTask

_T = TypeVar('_T')


class CrontabTaskBase(ImmediateTask[_T], ABC):
	def __init__(self, source: CommandSource, crontab_manager: CrontabManager, job_id: CrontabJobId):
		super().__init__(source)
		self.crontab_manager = crontab_manager
		self.job_id = job_id

	def get_job(self) -> CrontabJob:
		return self.crontab_manager.get_job(self.job_id)
