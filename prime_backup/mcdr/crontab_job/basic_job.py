import threading
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Optional, NamedTuple, Any

from apscheduler.job import Job
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.triggers.base import BaseTrigger
from apscheduler.triggers.interval import IntervalTrigger
from mcdreforged.api.all import *

from prime_backup import logger
from prime_backup.config.config import Config
from prime_backup.mcdr.crontab_job import CrontabJob, CrontabJobEvent
from prime_backup.mcdr.task import Task
from prime_backup.mcdr.task_queue import TaskQueue
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.units import Duration
from prime_backup.utils.mcdr_utils import broadcast_message, TranslationContext
from prime_backup.utils.waitable_value import WaitableValue

if TYPE_CHECKING:
	from prime_backup.mcdr.task_manager import TaskManager


class BasicCrontabJob(CrontabJob, TranslationContext, ABC):
	__base_tr = TranslationContext('job.base').tr

	def __init__(self, scheduler: BaseScheduler, task_manager: 'TaskManager'):
		CrontabJob.__init__(self, scheduler, task_manager)
		TranslationContext.__init__(self, f'job.{self.id.name}')
		self._root_config = Config.get()
		self.logger = logger.get()
		self.aps_job: Optional[Job] = None
		self.abort_event = threading.Event()

	# ============================= Job creation methods =============================

	def __ensure_aps_job(self):
		if self.aps_job is None:
			raise RuntimeError('job is not enabled yet')

	def _create_trigger(self) -> BaseTrigger:
		return IntervalTrigger(seconds=self.interval.value, jitter=self.jitter.value)

	@property
	@abstractmethod
	def interval(self) -> Duration:
		...

	@property
	@abstractmethod
	def jitter(self) -> Duration:
		...

	# ================================== Overrides ===================================

	def enable(self):
		if self.aps_job is not None:
			raise RuntimeError('double-enable a job')
		if self.is_enabled():
			trigger = self._create_trigger()
			self.aps_job = self.scheduler.add_job(func=self.run, trigger=trigger, id=self.id.name)

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

	def get_name_text(self) -> RTextBase:
		return self.tr('name').set_color(RColor.light_purple)

	def on_event(self, event: CrontabJobEvent):
		if event == CrontabJobEvent.plugin_unload:
			self.abort_event.set()

	# ==================================== Utils ====================================

	class RunTaskWithRetryResult(NamedTuple):
		executed: bool
		ret: Optional[Any]
		error: Optional[Exception]

	def run_task_with_retry(self, task: Task, can_retry: bool, delays: Optional[List[float]] = None, broadcast: bool = False, report_success: bool = True) -> RunTaskWithRetryResult:
		if delays is None:
			delays = [0, 10, 60]

		def log_info(msg: RTextBase):
			if broadcast:
				broadcast_message(msg)
			else:
				self.logger.info(msg.to_colored_text())

		def log_err(msg: RTextBase):
			if broadcast:
				broadcast_message(msg)
			else:
				self.logger.error(msg.to_colored_text())

		for delay in delays:
			self.abort_event.wait(delay)
			if self.abort_event.is_set():
				break
			try:
				def callback(*args):
					wv.set(args)
				wv = WaitableValue()
				self.task_manager.add_task(task, callback, handle_tmo_err=False)
			except TaskQueue.TooManyOngoingTask:
				if delay is None or not can_retry:  # <= 5min, no need to retry
					log_info(self.__base_tr('found_ongoing.skip'))
					break
				else:
					log_info(self.__base_tr('found_ongoing.wait_retry', TextComponents.number(f'{delay}s')))
			else:
				ret, err = wv.wait()
				if err is None:
					if report_success:
						log_info(self.__base_tr('completed', self.get_name_text(), self.get_next_run_date()))
				else:
					log_err(self.__base_tr('completed_with_error', self.get_name_text(), self.get_next_run_date()))
				return self.RunTaskWithRetryResult(True, ret, err)
		return self.RunTaskWithRetryResult(False, None, None)

	@classmethod
	def get_command_source(cls) -> CommandSource:
		from prime_backup.mcdr import mcdr_globals
		return mcdr_globals.server.get_plugin_command_source()
