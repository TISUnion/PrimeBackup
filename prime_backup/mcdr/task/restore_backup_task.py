import enum
import threading
from typing import Optional

from mcdreforged.api.all import *

from prime_backup.action.create_backup_action import CreateBackupAction
from prime_backup.action.export_backup_action import ExportBackupActions
from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.action.list_backup_action import ListBackupAction
from prime_backup.mcdr.task import TaskEvent, OperationTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.backup_tags import BackupTags, BackupTagName
from prime_backup.types.operator import Operator
from prime_backup.utils.mcdr_utils import click_and_run, mkcmd
from prime_backup.utils.timer import Timer
from prime_backup.utils.waitable_value import WaitableValue


class _ConfirmResult(enum.Enum):
	confirmed = enum.auto()
	cancelled = enum.auto()


class RestoreBackupTask(OperationTask):
	def __init__(self, source: CommandSource, backup_id: Optional[int] = None, skip_confirm: bool = False):
		super().__init__(source)
		self.backup_id = backup_id
		self.skip_confirm = skip_confirm
		self.confirm_result: WaitableValue[_ConfirmResult] = WaitableValue()
		self.abort_event = threading.Event()
		self.can_abort = False

	@property
	def name(self) -> str:
		return 'restore'

	def is_abort_able(self) -> bool:
		return self.can_abort

	def __countdown_and_stop_server(self, backup: BackupInfo) -> bool:
		for countdown in range(max(0, self.config.command.restore_countdown_sec), 0, -1):
			self.broadcast(click_and_run(
				self.tr('countdown', countdown, TextComponents.backup_brief(backup)),
				self.tr('countdown.hover', TextComponents.command('abort')),
				mkcmd('abort'),
			))

			if self.abort_event.wait(1):
				self.broadcast(self.tr('aborted'))
				return False

		self.server.stop()
		self.logger.info('Wait for server to stop')
		self.server.wait_until_stop()
		return True

	def run(self):
		if self.backup_id is None:
			backup_filter = BackupFilter()
			backup_filter.filter_non_pre_restore_backup()
			candidates = ListBackupAction(backup_filter=backup_filter, limit=1).run()
			if len(candidates) == 0:
				self.reply(self.tr('no_backup'))
				return
			backup = candidates[0]
		else:
			backup = GetBackupAction(self.backup_id).run()

		if not self.skip_confirm:
			confirm_time_wait = self.config.command.confirm_time_wait
			self.broadcast(self.tr('show_backup', TextComponents.backup_brief(backup)))
			self.broadcast(TextComponents.confirm_hint(self.tr('confirm_target'), TextComponents.duration(confirm_time_wait)))
			self.can_abort = True
			self.confirm_result.wait(confirm_time_wait.value)

			self.logger.info('confirm result: {}'.format(self.confirm_result))
			if not self.confirm_result.is_set():
				self.broadcast(self.tr('no_confirm'))
				return
			elif self.confirm_result.get() == _ConfirmResult.cancelled:
				self.broadcast(self.tr('aborted'))
				return

		if not self.__countdown_and_stop_server(backup):
			return

		self.can_abort = False
		timer = Timer()
		if self.config.command.backup_on_restore:
			self.logger.info('Creating backup of existing files to avoid idiot')
			CreateBackupAction(
				Operator.pb('pre_restore'),
				'Automatic backup before restoring to #{}'.format(backup.id),  # TODO: translate this
				tags=BackupTags().set(BackupTagName.pre_restore_backup, True),
			).run()
		cost_backup = timer.get_and_restart()

		self.logger.info('Restoring backup')
		ExportBackupActions.to_dir(backup.id, self.config.source_path, delete_existing=True).run()
		cost_restore = timer.get_and_restart()

		self.logger.info('Restore done, cost {}s (backup {}s, restore {}s), starting the server'.format(
			round(cost_backup + cost_restore, 2), round(cost_backup, 2), round(cost_restore, 2),
		))
		self.server.start()

	def on_event(self, event: TaskEvent):
		if event in [TaskEvent.plugin_unload, TaskEvent.operation_aborted]:
			self.confirm_result.set(_ConfirmResult.cancelled)
			self.abort_event.set()
		elif event == TaskEvent.operation_confirmed:
			self.confirm_result.set(_ConfirmResult.confirmed)
