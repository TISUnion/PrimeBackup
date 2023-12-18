import dataclasses
import datetime
import threading
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Optional, Any, Callable

from apscheduler.job import Job
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.triggers.base import BaseTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from mcdreforged.api.all import *
from typing_extensions import final

from prime_backup import logger
from prime_backup.config.config import Config
from prime_backup.config.config_common import CrontabJobSetting
from prime_backup.mcdr.crontab_job import CrontabJob, CrontabJobEvent
from prime_backup.mcdr.task import Task
from prime_backup.mcdr.task_queue import TaskQueue
from prime_backup.mcdr.text_components import TextComponents, TextColors
from prime_backup.types.units import Duration
from prime_backup.utils import misc_utils
from prime_backup.utils.mcdr_utils import broadcast_message, TranslationContext
from prime_backup.utils.waitable_value import WaitableValue

if TYPE_CHECKING:
	from prime_backup.mcdr.task_manager import TaskManager


class BasicCrontabJob(CrontabJob, TranslationContext, ABC):
	__base_tr = TranslationContext('job._base').tr

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

	def __ensure_running(self):
		self.__ensure_aps_job()
		if not self.is_running():
			raise RuntimeError('job is not running')

	def _create_trigger(self) -> BaseTrigger:
		if self.interval is not None:
			return IntervalTrigger(seconds=self.interval.value, jitter=self.jitter.value)
		elif self.crontab is not None:
			trigger = CronTrigger.from_crontab(self.crontab)
			trigger.jitter = self.jitter.value
			return trigger
		else:
			raise ValueError('no valid trigger for the job. is the config correct? config: {}'.format(self.job_config))

	@property
	@abstractmethod
	def job_config(self) -> CrontabJobSetting:
		...

	@final
	def is_enabled(self) -> bool:
		return self.job_config.enabled

	@final
	@property
	def interval(self) -> Optional[Duration]:
		return self.job_config.interval

	@final
	@property
	def crontab(self) -> Optional[str]:
		return self.job_config.crontab

	@final
	@property
	def jitter(self) -> Duration:
		return self.job_config.jitter

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

	def reschedule(self) -> bool:
		if self.aps_job is not None:
			self.aps_job.reschedule(self._create_trigger())
			return True
		return False

	def is_running(self) -> bool:
		return self.aps_job is not None and self.aps_job.next_run_time is not None

	def is_pause(self) -> bool:
		return not self.is_running()

	def get_seconds_until_next_run(self) -> float:
		self.__ensure_running()
		nrt = self.aps_job.next_run_time
		return (nrt - datetime.datetime.now(nrt.tzinfo)).total_seconds()

	def get_duration_until_next_run_text(self) -> RTextBase:
		self.__ensure_running()
		return TextComponents.date_diff(self.aps_job.next_run_time)

	def get_next_run_date(self) -> RTextBase:
		self.__ensure_aps_job()
		if (nrt := self.aps_job.next_run_time) is not None:
			return TextComponents.date(nrt)
		else:
			return self.__base_tr('next_run_date_paused').set_color(RColor.gray)

	def get_name_text(self) -> RTextBase:
		return self.tr('name').set_color(TextColors.job_id).h(self.id.name)

	def get_name_text_titled(self) -> RTextBase:
		return self.tr('name_titled').set_color(TextColors.job_id).h(self.id.name)

	def on_event(self, event: CrontabJobEvent):
		if event == CrontabJobEvent.plugin_unload:
			self.abort_event.set()

	# ==================================== Utils ====================================

	def __create_run_tasks_delays(self) -> List[int]:
		delays = [0]
		wait_max = self.get_seconds_until_next_run() * 0.2  # 20% of the minimum next run wait
		wait_sum = 0
		for d in [Duration('10s'), Duration('1m'), Duration('5m')]:
			wait_sum += d.value
			if wait_sum >= wait_max:
				break
			delays.append(d.value)
		return delays

	@dataclasses.dataclass(frozen=True)
	class RunTaskWithRetryResult(ABC):
		executed: bool
		ret: Optional[Any]
		error: Optional[Exception]

		@abstractmethod
		def report(self):
			"""
			Use this when the `run_task_with_retry` call is what the job does

			This method report the job execution result based on the method call result
			"""
			...

	def run_task_with_retry(
			self, task: Task, can_retry: bool, *,
			requirement: Callable[[], bool] = None,
			delays: Optional[List[float]] = None,
			broadcast: bool = False
	) -> RunTaskWithRetryResult:
		if delays is None:
			delays = self.__create_run_tasks_delays()
		misc_utils.assert_true(len(delays) > 0, 'delay should not be empty')

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

		this, base_tr = self, self.__base_tr

		class RunTaskWithRetryResultImpl(self.RunTaskWithRetryResult):
			def report(self):
				if self.executed:
					if self.error is None:
						log_info(base_tr('completed', this.get_name_text_titled(), this.get_next_run_date()))
					else:
						log_err(base_tr('completed_with_error', this.get_name_text_titled(), this.get_next_run_date()))
				else:
					log_info(base_tr('found_ongoing.skip', current_task, this.get_name_text()))

		for i, delay in enumerate(delays):
			self.abort_event.wait(delay)
			if self.abort_event.is_set():
				break
			if requirement is not None and not requirement():
				break
			try:
				def callback(*args):
					wv.set(args)
				wv = WaitableValue()
				self.task_manager.add_task(task, callback, handle_tmo_err=False)
			except TaskQueue.TooManyOngoingTask as e:
				current_task = e.current_item.task_name() if e.current_item is not None else self.tr('found_ongoing.unknown').set_color(RColor.gray)
				is_not_last = i < len(delays) - 1
				if is_not_last and can_retry:
					next_wait = delays[i + 1]
					log_info(self.__base_tr('found_ongoing.wait_retry', current_task, self.get_name_text(), TextComponents.number(f'{next_wait}s')))
				else:
					break
			else:
				ret, err = wv.wait()
				return RunTaskWithRetryResultImpl(True, ret, err)
		return RunTaskWithRetryResultImpl(False, None, None)

	@classmethod
	def get_command_source(cls) -> CommandSource:
		from prime_backup.mcdr import mcdr_globals
		return mcdr_globals.server.get_plugin_command_source()
