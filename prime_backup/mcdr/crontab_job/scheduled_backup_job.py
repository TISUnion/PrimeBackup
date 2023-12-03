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
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.operator import Operator
from prime_backup.utils.mcdr_utils import broadcast_message

if TYPE_CHECKING:
	from prime_backup.mcdr.task_manager import TaskManager


class ScheduledBackupJob(CrontabJob):
	def __init__(self, scheduler: BaseScheduler, task_manager: 'TaskManager'):
		super().__init__(scheduler, task_manager)
		self.config: ScheduledBackupConfig = self._root_config.scheduled_backup
		self.is_executing = threading.Event()
		self.is_aborted = threading.Event()

	@property
	def id(self) -> CrontabJobId:
		return CrontabJobId.scheduled_backup

	def _create_trigger(self) -> BaseTrigger:
		return IntervalTrigger(seconds=self.config.interval.value, jitter=self.config.jitter.value)

	def run(self):
		if not self.config.enabled:
			return

		if not mcdr_globals.server.is_server_running():
			return

		broadcast_message(self.tr('triggered', TextComponents.duration(self.config.interval)))
		with contextlib.ExitStack() as exit_stack:
			self.is_executing.set()
			exit_stack.callback(self.is_executing.clear)

			source = mcdr_globals.server.get_plugin_command_source()
			task = CreateBackupTask(source, self.tr('comment').to_plain_text(), operator=Operator.pb('scheduled_backup'))
			self.run_task_with_retry(task, self.config.interval.value > 300)  # task <= 5min, no need to retry

	def enable(self, *args, **kwargs):
		if self.config.enabled:
			super().enable(*args, **kwargs)
			self.aps_job.modify(max_instances=1)

	def on_event(self, event: CrontabJobEvent):
		super().on_event(event)

		if not self.config.enabled:
			return

		if event == CrontabJobEvent.manual_backup_created:
			if not self.is_executing.is_set() and self.config.reset_timer_on_backup:
				self.aps_job.reschedule(self._create_trigger())
				broadcast_message(self.tr('reset_on_backup', self.get_next_run_date()))
