from typing import Optional

from mcdreforged.api.all import *

from prime_backup.action.create_backup_action import CreateBackupAction
from prime_backup.action.export_backup_action import ExportBackupToDirectoryAction
from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.action.list_backup_action import ListBackupAction
from prime_backup.mcdr.task.basic_task import OperationTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.backup_tags import BackupTags, BackupTagName
from prime_backup.types.operator import Operator, PrimeBackupOperatorNames
from prime_backup.utils.mcdr_utils import click_and_run, mkcmd
from prime_backup.utils.timer import Timer


class RestoreBackupTask(OperationTask[None]):
	def __init__(self, source: CommandSource, backup_id: Optional[int] = None, needs_confirm: bool = True, fail_soft: bool = False):
		super().__init__(source)
		self.backup_id = backup_id
		self.needs_confirm = needs_confirm
		self.fail_soft = fail_soft

	@property
	def id(self) -> str:
		return 'backup_restore'

	def __countdown_and_stop_server(self, backup: BackupInfo) -> bool:
		for countdown in range(max(0, self.config.command.restore_countdown_sec), 0, -1):
			self.broadcast(click_and_run(
				RText('!!! ', RColor.red) + self.tr('countdown', countdown, TextComponents.backup_brief(backup, backup_id_fancy=False)),
				self.tr('countdown.hover', TextComponents.command('abort')),
				mkcmd('abort'),
			))

			if self.aborted_event.wait(1):
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

		if self.needs_confirm:
			self.broadcast(self.tr('show_backup', TextComponents.backup_brief(backup)))
			cr = self.wait_confirm(self.tr('confirm_target'))
			if not cr.is_set():
				self.broadcast(self.tr('no_confirm'))
				return
			elif cr.get().is_cancelled():
				self.broadcast(self.tr('aborted'))
				return

		server_was_running = self.server.is_server_running()
		if server_was_running:
			if not self.__countdown_and_stop_server(backup):
				return
		else:
			self.logger.info('Found an already-stopped server')

		timer = Timer()
		if self.config.command.backup_on_restore:
			self.logger.info('Creating backup of existing files to avoid idiot')
			CreateBackupAction(
				Operator.pb(PrimeBackupOperatorNames.pre_restore),
				self.tr('pre_restore_comment', backup.id).to_plain_text(),
				tags=BackupTags().set(BackupTagName.pre_restore_backup, True),
			).run()
		cost_backup = timer.get_and_restart()

		self.logger.info('Restoring backup (fail-soft={})'.format(self.fail_soft))
		ExportBackupToDirectoryAction(backup.id, self.config.source_path, delete_existing=True, fail_soft=self.fail_soft).run()
		cost_restore = timer.get_and_restart()

		self.logger.info('Restore done, cost {}s (backup {}s, restore {}s), starting the server'.format(
			round(cost_backup + cost_restore, 2), round(cost_backup, 2), round(cost_restore, 2),
		))

		if server_was_running:
			self.server.start()
