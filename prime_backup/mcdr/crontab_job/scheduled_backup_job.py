import contextlib
import threading
from typing import TYPE_CHECKING

from apscheduler.schedulers.base import BaseScheduler
from apscheduler.triggers.base import BaseTrigger
from apscheduler.triggers.interval import IntervalTrigger

from prime_backup.config.sub_configs import ScheduledBackupConfig
from prime_backup.mcdr import mcdr_globals
from prime_backup.mcdr.crontab_job import CrontabJob, CrontabJobEvent, CrontabJobId
from prime_backup.mcdr.task.create_backup_task import CreateBackupTask
from prime_backup.mcdr.task_queue import TaskQueue
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.operator import Operator
from prime_backup.utils.mcdr_utils import broadcast_message
from prime_backup.utils.waitable_value import WaitableValue

if TYPE_CHECKING:
	from prime_backup.mcdr.task_manager import TaskManager


class ScheduledBackupJob(CrontabJob):
	def __init__(self, scheduler: BaseScheduler, task_manager: 'TaskManager'):
		super().__init__(scheduler, task_manager)
		self.config: ScheduledBackupConfig = self.config.scheduled_backup
		self.is_executing = threading.Event()
		self.is_aborted = threading.Event()

	@property
	def id(self) -> CrontabJobId:
		return CrontabJobId.scheduled_backup

	def _create_trigger(self) -> BaseTrigger:
		return IntervalTrigger(seconds=self.config.interval.value)

	def _run(self):
		if not self.config.enabled:
			return

		if not mcdr_globals.server.is_server_running():
			return

		broadcast_message(self.tr('triggered', TextComponents.duration(self.config.interval)))
		source = mcdr_globals.server.get_plugin_command_source()

		with contextlib.ExitStack() as es:
			self.is_executing.set()
			es.callback(lambda: self.is_executing.clear())

			for delay in [10, 60, None]:
				if self.is_aborted.is_set():
					break
				try:
					wv = WaitableValue()
					self.task_manager.add_task(
						CreateBackupTask(source, self.tr('comment').to_plain_text(), operator=Operator.pb('scheduled_backup')),
						wv.set, handle_tmo_err=False,
					)
				except TaskQueue.TooManyOngoingTask:
					if delay is None or self.config.interval.value <= 300:  # <= 5min, no need to retry
						broadcast_message(self.tr('found_ongoing.skip'))
						break
					else:
						broadcast_message(self.tr('found_ongoing.wait_retry', TextComponents.duration(delay)))
						self.is_aborted.wait(delay)
				else:
					err = wv.wait()
					if err is None:
						broadcast_message(self.tr('completed', self.get_next_run_date()))
					else:
						broadcast_message(self.tr('completed_with_error', self.get_next_run_date()))
					break

	def enable(self, *args, **kwargs):
		if self.config.enabled:
			super().enable(*args, **kwargs)
			self.aps_job.modify(max_instances=1)

	def on_event(self, event: CrontabJobEvent):
		if not self.config.enabled:
			return

		if event == CrontabJobEvent.plugin_unload:
			self.is_aborted.set()
		if event == CrontabJobEvent.manual_backup_created:
			if not self.is_executing.is_set() and self.config.reset_timer_on_backup:
				self.aps_job.reschedule(self._create_trigger())
				broadcast_message(self.tr('reset_on_backup', self.get_next_run_date()))

