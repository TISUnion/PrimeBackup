import contextlib
import threading
from typing import TYPE_CHECKING

from apscheduler.schedulers.base import BaseScheduler

from prime_backup.config.scheduled_backup import ScheduledBackupConfig
from prime_backup.mcdr import mcdr_globals
from prime_backup.mcdr.crontab_job import CrontabJobEvent, CrontabJobId
from prime_backup.mcdr.crontab_job.basic_job import BasicCrontabJob
from prime_backup.mcdr.task.backup.create_backup_task import CreateBackupTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.operator import Operator, PrimeBackupOperatorNames
from prime_backup.types.units import Duration
from prime_backup.utils.mcdr_utils import broadcast_message

if TYPE_CHECKING:
	from prime_backup.mcdr.task_manager import TaskManager


class ScheduledBackupJob(BasicCrontabJob):
	def __init__(self, scheduler: BaseScheduler, task_manager: 'TaskManager'):
		super().__init__(scheduler, task_manager)
		self.config: ScheduledBackupConfig = self._root_config.scheduled_backup
		self.is_executing = threading.Event()
		self.is_aborted = threading.Event()

	@property
	def id(self) -> CrontabJobId:
		return CrontabJobId.schedule_backup

	@property
	def interval(self) -> Duration:
		return self.config.interval

	@property
	def jitter(self) -> Duration:
		return self.config.jitter

	def is_enabled(self) -> bool:
		return self.config.enabled

	def run(self):
		if not self.config.enabled:
			return

		if not mcdr_globals.server.is_server_running():
			return

		broadcast_message(self.tr('triggered', TextComponents.duration(self.config.interval)))
		with contextlib.ExitStack() as exit_stack:
			self.is_executing.set()
			exit_stack.callback(self.is_executing.clear)

			t_comment = self.tr('comment')
			if getattr(t_comment, 'translation_key') != t_comment.to_plain_text():
				comment = t_comment.to_plain_text()  # translation exists
			else:
				comment = 'Scheduled Backup'
			task = CreateBackupTask(self.get_command_source(), comment, operator=Operator.pb(PrimeBackupOperatorNames.scheduled_backup))
			self.run_task_with_retry(task, self.config.interval.value > 300)  # task <= 5min, no need to retry

	def on_event(self, event: CrontabJobEvent):
		super().on_event(event)

		if not self.config.enabled:
			return

		if event == CrontabJobEvent.manual_backup_created:
			if not self.is_executing.is_set() and self.config.reset_timer_on_backup:
				self.aps_job.reschedule(self._create_trigger())
				broadcast_message(self.tr('reset_on_backup', self.get_next_run_date()))
