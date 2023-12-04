import enum

from mcdreforged.api.all import *

from prime_backup.mcdr.crontab_job import CrontabJobId, CrontabJob
from prime_backup.mcdr.crontab_manager import CrontabManager
from prime_backup.mcdr.task.crontab import CrontabTaskBase


class OperateCrontabJobTask(CrontabTaskBase):
	class Operation(enum.Enum):
		pause = enum.auto()
		resume = enum.auto()
	
	def __init__(self, source: CommandSource, crontab_manager: CrontabManager, job_id: CrontabJobId, operation: Operation):
		super().__init__(source, crontab_manager, job_id)
		self.operation = operation

	@property
	def name(self) -> str:
		return 'crontab_operate'

	def run(self) -> None:
		job = self.get_job()
		if self.operation == self.Operation.pause:
			self.__pause(job)
		elif self.operation == self.Operation.resume:
			self.__resume(job)
		else:
			raise ValueError(self.operation)

	def __pause(self, job: CrontabJob):
		if job.is_pause():
			self.reply(self.tr('pause.already', job.get_name_text()))
		else:
			job.pause()
			self.reply(self.tr('pause.done', job.get_name_text()))
			self.reply(self.tr('pause.done.notes'))

	def __resume(self, job: CrontabJob):
		if job.is_pause():
			job.resume()
			self.reply(self.tr('resume.done', job.get_name_text(), job.get_next_run_date()))
		else:
			self.reply(self.tr('resume.already', job.get_name_text(), job.get_next_run_date()))
