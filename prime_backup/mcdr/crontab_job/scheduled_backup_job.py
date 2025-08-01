import contextlib
import threading
from typing import TYPE_CHECKING

from apscheduler.schedulers.base import BaseScheduler
from typing_extensions import override

from prime_backup.config.config_common import CrontabJobSetting
from prime_backup.config.scheduled_backup_config import ScheduledBackupConfig
from prime_backup.mcdr import mcdr_globals
from prime_backup.mcdr.crontab_job import CrontabJobEvent, CrontabJobId
from prime_backup.mcdr.crontab_job.basic_job import BasicCrontabJob
from prime_backup.mcdr.online_player_counter import OnlinePlayerCounter
from prime_backup.mcdr.task.backup.create_backup_task import CreateBackupTask
from prime_backup.types.operator import Operator, PrimeBackupOperatorNames
from prime_backup.utils import backup_utils
from prime_backup.utils.mcdr_utils import broadcast_message

if TYPE_CHECKING:
	from prime_backup.mcdr.task_manager import TaskManager


class ScheduledBackupJob(BasicCrontabJob):
	def __init__(self, scheduler: BaseScheduler, task_manager: 'TaskManager'):
		super().__init__(scheduler, task_manager)
		self.config: ScheduledBackupConfig = self._root_config.scheduled_backup
		self.is_executing = threading.Event()
		self.is_aborted = threading.Event()
		self.found_created_backup = threading.Event()

	@property
	@override
	def id(self) -> CrontabJobId:
		return CrontabJobId.schedule_backup

	@property
	@override
	def job_config(self) -> CrontabJobSetting:
		return self.config

	@override
	def run(self):
		if not self.config.enabled:
			return

		if not mcdr_globals.server.is_server_running():
			return

		if self.config.require_online_players:
			online_player_counter = OnlinePlayerCounter.get()
			player_records = online_player_counter.get_player_records()

			if player_records is not None:
				base_msg = "Scheduled backup player check: valid {}, ignored {}".format(
					[
						"*" + record.name if record.online else record.name
						for record in player_records.values()
						if record.valid
					],
					[
						"*" + record.name if record.online else record.name
						for record in player_records.values()
						if not record.valid
					],
				)
				if not any(record.valid for record in player_records.values()):
					self.logger.info("{}, backup skipped".format(base_msg))
					return

				if not any(
					record.valid and record.online for record in player_records.values()
				):
					self.logger.info(
						"{}, no valid online player, performing the last backup".format(
							base_msg
						)
					)
				else:
					self.logger.info("{}, performing normally".format(base_msg))

				online_player_counter.reset_player_records()

			else:
				base_msg = "Scheduled backup player check: no valid data"
				self.logger.info("{}, performing normally".format(base_msg))

		broadcast_message(self.tr('triggered', self.get_name_text_titled()))
		with contextlib.ExitStack() as exit_stack:
			self.is_executing.set()
			exit_stack.callback(self.is_executing.clear)

			comment = backup_utils.create_translated_backup_comment('scheduled_backup')
			operator = Operator.pb(PrimeBackupOperatorNames.scheduled_backup)
			task = CreateBackupTask(self.get_command_source(), comment, operator=operator)

			self.found_created_backup.clear()
			self.run_task_with_retry(task, True, requirement=lambda: not self.found_created_backup.is_set(), broadcast=True).report()

	@override
	def on_event(self, event: CrontabJobEvent):
		super().on_event(event)

		if not self.config.enabled:
			return

		if event == CrontabJobEvent.manual_backup_created:
			self.found_created_backup.set()
			if not self.is_executing.is_set() and self.config.reset_timer_on_backup:
				if self.interval is not None:  # reset for interval mode only
					ok = self.reschedule()
					if ok:
						broadcast_message(self.tr('reset_on_backup', self.get_next_run_date()))
