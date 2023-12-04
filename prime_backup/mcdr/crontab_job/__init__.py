import enum
import threading
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Optional, NamedTuple

from apscheduler.job import Job
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.triggers.base import BaseTrigger
from mcdreforged.api.all import *

from prime_backup import logger
from prime_backup.config.config import Config
from prime_backup.mcdr.task import Task
from prime_backup.mcdr.task_queue import TaskQueue
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.utils.mcdr_utils import broadcast_message, TranslationContext
from prime_backup.utils.waitable_value import WaitableValue

if TYPE_CHECKING:
	from prime_backup.mcdr.task_manager import TaskManager


class CrontabJobId(enum.Enum):
	schedule_backup = enum.auto()
	prune_backup = enum.auto()


class CrontabJobEvent(enum.Enum):
	plugin_unload = enum.auto()
	manual_backup_created = enum.auto()


class CrontabJob(TranslationContext, ABC):
	__base_tr = TranslationContext('job.base').tr

	def __init__(self, scheduler: BaseScheduler, task_manager: 'TaskManager'):
		super().__init__(f'job.{self.id.name}')
		self.scheduler = scheduler
		self.task_manager = task_manager
		self._root_config = Config.get()
		self.logger = logger.get()
		self.aps_job: Optional[Job] = None
		self.abort_event = threading.Event()

	def __ensure_aps_job(self):
		if self.aps_job is None:
			raise RuntimeError('job is not enabled yet')

	def enable(self, trigger: Optional[BaseTrigger] = None):
		if self.aps_job is not None:
			raise RuntimeError('double-enable a job')
		if trigger is None:
			trigger = self._create_trigger()
		self.aps_job = self.scheduler.add_job(func=self.run, trigger=trigger, id=self.id.name)
		# self.logger.info('Job %s enabled. Next run date: %s',  self.id, self.get_next_run_date())

	def pause(self):
		self.__ensure_aps_job()
		self.aps_job.pause()

	def resume(self):
		self.__ensure_aps_job()
		self.aps_job.resume()

	def is_running(self) -> bool:
		return self.aps_job.next_run_time is not None

	def is_pause(self) -> bool:
		return not self.is_running()

	def get_next_run_date(self) -> RTextBase:
		self.__ensure_aps_job()
		if (nrt := self.aps_job.next_run_time) is not None:
			return TextComponents.date(nrt)
		else:
			return self.__base_tr('next_run_date_paused').set_color(RColor.gray)

	class RunTaskWithRetryResult(NamedTuple):
		executed: bool
		error: Optional[Exception]

	def run_task_with_retry(self, task: Task, can_retry: bool, delays: Optional[List[float]] = None) -> RunTaskWithRetryResult:
		if delays is None:
			delays = [0, 10, 60]
		for delay in delays:
			self.abort_event.wait(delay)
			if self.abort_event.is_set():
				break
			try:
				wv = WaitableValue()
				self.task_manager.add_task(task, wv.set, handle_tmo_err=False)
			except TaskQueue.TooManyOngoingTask:
				if delay is None or not can_retry:  # <= 5min, no need to retry
					broadcast_message(self.__base_tr('found_ongoing.skip'))
					break
				else:
					broadcast_message(self.__base_tr('found_ongoing.wait_retry', TextComponents.number(f'{delay}s')))
			else:
				err = wv.wait()
				if err is None:
					broadcast_message(self.__base_tr('completed', self.get_next_run_date()))
				else:
					broadcast_message(self.__base_tr('completed_with_error', self.get_next_run_date()))
				return self.RunTaskWithRetryResult(True, err)
		return self.RunTaskWithRetryResult(False, None)

	def get_name_text(self) -> RTextBase:
		return self.tr('name').set_color(RColor.light_purple)

	@property
	@abstractmethod
	def id(self) -> CrontabJobId:
		...

	@abstractmethod
	def _create_trigger(self) -> BaseTrigger:
		...

	@abstractmethod
	def run(self):
		...

	def on_event(self, event: CrontabJobEvent):
		if event == CrontabJobEvent.plugin_unload:
			self.abort_event.set()
