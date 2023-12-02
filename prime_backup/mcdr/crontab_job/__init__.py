import enum
from abc import ABC, abstractmethod
from typing import Optional, TYPE_CHECKING

from apscheduler.job import Job
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.triggers.base import BaseTrigger
from mcdreforged.minecraft.rtext.text import RTextBase

from prime_backup import logger
from prime_backup.config.config import Config
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.utils.mcdr_utils import TranslationContext

if TYPE_CHECKING:
	from prime_backup.mcdr.task_manager import TaskManager


class CrontabJobId(enum.Enum):
	scheduled_backup = enum.auto()
	prune_backup = enum.auto()


class CrontabJobEvent(enum.Enum):
	plugin_unload = enum.auto()
	manual_backup_created = enum.auto()


class CrontabJob(TranslationContext, ABC):
	def __init__(self, scheduler: BaseScheduler, task_manager: 'TaskManager'):
		super().__init__(f'job.{self.id.name}')
		self.scheduler = scheduler
		self.task_manager = task_manager
		self.config = Config.get()
		self.logger = logger.get()
		self.aps_job: Optional[Job] = None

	def enable(self, trigger: Optional[BaseTrigger] = None):
		if self.aps_job is not None:
			raise RuntimeError('double-enable a job')
		if trigger is None:
			trigger = self._create_trigger()
		self.aps_job = self.scheduler.add_job(func=self._run, trigger=trigger, id=self.id.name)

	def get_next_run_date(self) -> RTextBase:
		if self.aps_job is None:
			raise RuntimeError('job is not enabled yet')
		return TextComponents.date(self.aps_job.next_run_time)

	@property
	@abstractmethod
	def id(self) -> CrontabJobId:
		...

	@abstractmethod
	def _create_trigger(self) -> BaseTrigger:
		...

	@abstractmethod
	def _run(self):
		...

	def on_event(self, event: CrontabJobEvent):
		pass
