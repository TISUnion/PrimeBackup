import enum
import threading
from typing import Optional

from mcdreforged.api.all import *

from prime_backup.action.create_backup_action import CreateBackupAction
from prime_backup.action.export_backup_action import ExportBackupActions
from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.action.list_backup_action import ListBackupAction
from prime_backup.config.config import Config
from prime_backup.config.types import Duration
from prime_backup.mcdr.task import TaskEvent, OperationTask
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.operator import Operator
from prime_backup.utils.mcdr_utils import click_and_run, mkcmd, Texts
from prime_backup.utils.waitable_value import WaitableValue


class _ConfirmResult(enum.Enum):
	confirmed = enum.auto()
	cancelled = enum.auto()


class RestoreBackupTask(OperationTask):
	def __init__(self, source: CommandSource, backup_id: Optional[int] = None):
		super().__init__(source)
		self.backup_id = backup_id
		self.confirm_result: WaitableValue[_ConfirmResult] = WaitableValue()
		self.abort_event = threading.Event()

	@property
	def name(self) -> str:
		return 'restore'

	def __countdown_and_stop_server(self, backup: BackupInfo) -> bool:
		for countdown in range(10, 0, -1):
			self.broadcast(click_and_run(
				self.tr('countdown', countdown, Texts.backup(backup)),
				self.tr('countdown.hover'),
				mkcmd('abort')
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
			candidates = ListBackupAction(limit=1).run()
			if len(candidates) == 0:
				self.reply(self.tr('no_backup'))
			backup = candidates[0]
		else:
			backup = GetBackupAction(self.backup_id).run()

		confirm_time_wait = self.config.command.confirm_time_wait
		self.broadcast(self.tr('show_backup', Texts.backup(backup)))
		self.broadcast(self.tr(
			'confirm_hint.base',
			confirm_time_wait.lower(),
			click_and_run(
				self.tr('confirm_hint.confirm', Texts.command('confirm')).set_color(RColor.red),
				self.tr('confirm_hint.confirm.hover', Texts.command('confirm')),
				mkcmd('confirm'),
			),
			click_and_run(
				self.tr('confirm_hint.abort', Texts.command('abort')).set_color(RColor.yellow),
				self.tr('confirm_hint.abort.hover', Texts.command('abort')),
				mkcmd('abort'),
			),
		))
		self.confirm_result.wait(Duration(confirm_time_wait).duration)

		self.logger.info('confirm result: {}'.format(self.confirm_result))
		if not self.confirm_result.is_set():
			self.broadcast(self.tr('no_confirm'))
			return
		elif self.confirm_result.get() == _ConfirmResult.cancelled:
			self.broadcast(self.tr('aborted'))
			return

		if not self.__countdown_and_stop_server(backup):
			return

		if Config.get().backup.backup_on_overwrite:
			self.logger.info('Creating backup of existing files to avoid idiot')
			CreateBackupAction(
				Operator.pb('pre_restore'),
				'Automatic backup before restoring to #{}'.format(backup.id),
				hidden=True,
			).run()

		self.logger.info('Restoring backup')
		ExportBackupActions.to_dir(backup.id, Config.get().source_path, delete_existing=True).run()

		self.logger.info('Restore done, starting the server')
		self.server.start()

	def on_event(self, event: TaskEvent):
		if event == TaskEvent.plugin_unload:
			self.confirm_result.set(_ConfirmResult.cancelled)
			self.abort_event.set()
		elif event == TaskEvent.operation_confirmed:
			self.confirm_result.set(_ConfirmResult.confirmed)
		elif event == TaskEvent.operation_aborted:
			self.confirm_result.set(_ConfirmResult.cancelled)
			self.abort_event.set()
