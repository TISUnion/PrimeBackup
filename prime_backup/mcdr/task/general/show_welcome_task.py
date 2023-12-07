from typing import Dict, Union

from mcdreforged.api.all import *

from prime_backup.action.list_backup_action import ListBackupAction
from prime_backup.mcdr import mcdr_globals
from prime_backup.mcdr.task.basic_task import ReaderTask
from prime_backup.mcdr.task.general import help_message_utils
from prime_backup.mcdr.task.general.show_help_task import ShowHelpTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.utils.mcdr_utils import mkcmd


class ShowWelcomeTask(ReaderTask):
	BACKUP_NUMBER_TO_SHOW = 3
	COMMON_COMMANDS = ['', 'make', 'back', 'list', 'show', 'rename', 'delete', 'confirm', 'abort']

	@property
	def name(self) -> str:
		return 'welcome'

	@property
	def __cmd_prefix(self) -> str:
		return self.config.command.prefix

	def __generate_command_helps(self) -> Dict[str, RTextBase]:
		msg = ShowHelpTask(self.source).tr('commands.content', prefix=self.__cmd_prefix)
		with self.source.preferred_language_context():
			return {h.literal: h.text for h in help_message_utils.parse_help_message(msg)}

	def reply(self, msg: Union[str, RTextBase], *, with_prefix: bool = False):
		super().reply(msg, with_prefix=with_prefix)

	def run(self) -> None:
		self.reply(TextComponents.title(self.tr(
			'title',
			name=RText(mcdr_globals.metadata.name, RColor.dark_aqua),
			version=mcdr_globals.metadata.version,
		)))
		self.reply(mcdr_globals.metadata.get_description_rtext())

		self.reply(
			self.tr('common_commands').
			set_color(RColor.light_purple).
			h(self.tr('common_commands.hover', TextComponents.command('help'))).
			c(RAction.run_command, mkcmd('help'))
		)
		helps = self.__generate_command_helps()
		for cmd in self.COMMON_COMMANDS:
			self.reply(helps[cmd])

		backup_filter = BackupFilter()
		backup_filter.filter_non_pre_restore_backup()
		backups = ListBackupAction(backup_filter=backup_filter, limit=self.BACKUP_NUMBER_TO_SHOW, calc_size=False).run()
		self.reply(self.tr('recent_backups', len(backups)).set_color(RColor.light_purple))
		for backup in backups:
			self.reply(TextComponents.backup_full(backup, operation_buttons=True))

		self.reply(self.tr('quick_actions.title').set_color(RColor.light_purple))
		with self.source.preferred_language_context():
			buttons = [
				RTextList('[', self.tr('quick_actions.create'), ']').
				set_color(RColor.green).
				h(TextComponents.command('make ')).
				c(RAction.suggest_command, mkcmd('make ' + self.tr('quick_actions.create.comment').to_plain_text()))
			]
		if len(backups) > 0:
			buttons.append(
				RTextList('[', self.tr('quick_actions.restore', TextComponents.backup_brief(backups[0])), ']').
				set_color(RColor.red).
				h(TextComponents.command('back')).
				c(RAction.suggest_command, mkcmd('back'))
			)
		self.reply(RTextBase.join(' ', buttons))