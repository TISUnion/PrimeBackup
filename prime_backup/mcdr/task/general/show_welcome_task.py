from typing import Dict, Union

from mcdreforged.api.all import *

from prime_backup.action.list_backup_action import ListBackupAction
from prime_backup.mcdr import mcdr_globals
from prime_backup.mcdr.task.basic_task import LightTask
from prime_backup.mcdr.task.general import help_message_utils
from prime_backup.mcdr.task.general.show_help_task import ShowHelpTask
from prime_backup.mcdr.text_components import TextComponents, TextColors
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.utils.mcdr_utils import mkcmd


class ShowWelcomeTask(LightTask[None]):
	BACKUP_NUMBER_TO_SHOW = 3
	COMMON_COMMANDS = ['', 'help', 'make', 'back', 'list', 'show', 'rename', 'delete', 'confirm', 'abort']

	@property
	def id(self) -> str:
		return 'welcome'

	def reply(self, msg: Union[str, RTextBase], *, with_prefix: bool = False):
		super().reply(msg, with_prefix=with_prefix)

	@property
	def __cmd_prefix(self) -> str:
		return self.config.command.prefix

	def __generate_command_helps(self) -> Dict[str, RTextBase]:
		msg = ShowHelpTask(self.source).tr('commands.content', prefix=self.__cmd_prefix)
		with self.source.preferred_language_context():
			return {h.literal: h.text for h in help_message_utils.parse_help_message(msg)}

	def run(self) -> None:
		self.reply(TextComponents.title(self.tr(
			'title',
			name=RText(mcdr_globals.metadata.name, RColor.dark_aqua),
			version=RText(f'v{mcdr_globals.metadata.version}', RColor.gold),
		)))
		self.reply(mcdr_globals.metadata.get_description_rtext())

		self.reply(
			self.tr('common_commands').
			set_color(TextColors.help_title).
			h(self.tr('common_commands.hover', TextComponents.command('help'))).
			c(RAction.suggest_command, mkcmd('help'))
		)
		helps = self.__generate_command_helps()
		for cmd in self.COMMON_COMMANDS:
			self.reply(helps[cmd])

		backup_filter = BackupFilter()
		backup_filter.filter_non_hidden_backup()
		backup_filter.filter_non_temporary_backup()
		backups = ListBackupAction(backup_filter=backup_filter, limit=self.BACKUP_NUMBER_TO_SHOW).run()
		self.reply(self.tr('recent_backups', len(backups)).set_color(TextColors.help_title))
		for backup in backups:
			self.reply(TextComponents.backup_full(backup, operation_buttons=True))

		self.reply(self.tr('quick_actions.title').set_color(TextColors.help_title))
		with self.source.preferred_language_context():
			buttons = [
				RTextList('[', self.tr('quick_actions.create'), ']').
				set_color(RColor.green).
				h(TextComponents.command('make')).
				c(RAction.suggest_command, mkcmd('make ' + self.tr('quick_actions.create.comment').to_plain_text()))
			]
		if len(backups) > 0:
			buttons.append(
				RTextList('[', self.tr('quick_actions.restore', TextComponents.backup_brief(backups[0])), ']').
				set_color(RColor.red).
				h(RTextBase.join('\n', [TextComponents.command('back'), self.tr('quick_actions.restore_explain')])).
				c(RAction.suggest_command, mkcmd('back'))
			)
		self.reply(RTextBase.join(' ', buttons))
