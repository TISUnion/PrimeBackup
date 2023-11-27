import enum
import threading

from mcdreforged.command.command_source import CommandSource

from xbackup.config.config import Config
from xbackup.config.types import Duration
from xbackup.db.access import DbAccess
from xbackup.task.core.create_backup_task import CreateBackupTask
from xbackup.task.core.export_backup_task import ExportBackupTasks
from xbackup.task.event import TaskEvent
from xbackup.task.task import Task
from xbackup.task.types.backup_info import BackupInfo
from xbackup.task.types.operator import Operator
from xbackup.utils.mcdr_utils import print_message, command_run, tr
from xbackup.utils.waitable_value import WaitableValue


class _ConfirmResult(enum.Enum):
	confirmed = enum.auto()
	cancelled = enum.auto()


class RestoreServerBackupTask(Task):
	def __init__(self, source: CommandSource, backup_id: int):
		super().__init__()
		self.source = source
		self.backup_id = backup_id
		self.confirm_result: WaitableValue[_ConfirmResult] = WaitableValue()
		self.abort_event = threading.Event()

	def __countdown_and_stop_server(self, backup: BackupInfo) -> bool:
		for countdown in range(1, 10):
			print_message(self.source, command_run(
				tr('do_restore.countdown.text', 10 - countdown, backup.pretty_text(False)),
				tr('do_restore.countdown.hover'),
				'{} abort'.format(Config.command.prefix)
			), tell=False)

			if self.abort_event.wait(1):
				print_message(self.source, tr('do_restore.abort'), tell=False)
				return False

		self.source.get_server().stop()
		self.logger.info('Wait for server to stop')
		self.source.get_server().wait_until_stop()
		return True

	def run(self):
		# ensure backup exists first
		with DbAccess.open_session() as session:
			backup = session.get_backup_or_throw(self.backup_id)
			backup = BackupInfo.of(backup)

		confirm_time_wait = Duration('60s')  # TODO: make it configurable?
		self.confirm_result.wait(confirm_time_wait.duration)

		self.logger.info('confirm result: {}'.format(self.confirm_result))
		if not self.confirm_result.is_set():
			self.source.reply('No confirm, restore task cancelled')
			return
		elif self.confirm_result == _ConfirmResult.cancelled:
			self.source.reply('Aborted')
			return

		if not self.__countdown_and_stop_server(backup):
			return

		if Config.get().backup.backup_on_overwrite:
			self.logger.info('Creating backup of existing files to avoid idiot')
			cbt = CreateBackupTask(Operator.xbackup('pre_restore'), 'Automatic backup before restoring to backup {}, executed by {}'.format(self.backup_id, self.source))
			cbt.run()

		self.logger.info('Restoring backup')
		ExportBackupTasks.to_dir(self.backup_id, Config.get().source_path, delete_existing=True).run()

	def on_event(self, event: TaskEvent):
		if event == TaskEvent.operation_confirmed:
			self.confirm_result.set(_ConfirmResult.confirmed)
		if event == TaskEvent.operation_cancelled:
			self.confirm_result.set(_ConfirmResult.cancelled)
		if event == TaskEvent.operation_aborted:
			self.abort_event.set()
