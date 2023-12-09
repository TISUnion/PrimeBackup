from typing import Optional, Union

from mcdreforged.api.all import *

from prime_backup import constants
from prime_backup.mcdr import mcdr_globals
from prime_backup.mcdr.crontab_job import CrontabJobId
from prime_backup.mcdr.task.basic_task import ImmediateTask
from prime_backup.mcdr.task.general import help_message_utils
from prime_backup.mcdr.text_components import TextComponents, TextColors
from prime_backup.utils.mcdr_utils import mkcmd


class ShowHelpTask(ImmediateTask[None]):
	COMMANDS_WITH_DETAILED_HELP = [
		'back',
		'crontab',
		'database',
		'export',
		'list',
		'tag',
	]

	def __init__(self, source: CommandSource, what: Optional[str] = None):
		super().__init__(source)
		self.what = what

	@property
	def id(self) -> str:
		return 'help'

	def reply(self, msg: Union[str, RTextBase], *, with_prefix: bool = False):
		super().reply(msg, with_prefix=with_prefix)

	@property
	def __cmd_prefix(self) -> str:
		return self.config.command.prefix

	def __reply_help(self, msg: RTextBase, hide_for_permission: bool = False):
		for h in help_message_utils.parse_help_message(msg):
			if hide_for_permission and h.is_help() and not self.source.has_permission(h.permission):
				continue
			self.reply(h.text)

	def __has_permission(self, literal: str) -> bool:
		return self.source.has_permission(self.config.command.permission.get(literal))

	def run(self) -> None:
		with self.source.preferred_language_context():
			if self.what is None:
				self.reply(self.tr('commands.title').set_color(TextColors.help_title))
				self.__reply_help(self.tr('commands.content', prefix=self.__cmd_prefix), True)
				self.reply(self.tr('arguments.title').set_color(TextColors.help_title))
				self.__reply_help(self.tr('arguments.content'))

				self.reply(self.tr('other.title').set_color(TextColors.help_title))
				self.reply(self.tr(
					'other.nodes_with_help',
					RTextBase.join(
						RText(', ', RColor.dark_gray),
						[
							RText(cmd, RColor.gray).
							h(TextComponents.command(f'help {cmd}')).
							c(RAction.suggest_command, mkcmd(f'help {cmd}'))
							for cmd in self.COMMANDS_WITH_DETAILED_HELP
							if self.__has_permission(cmd)
						]
					)
				))
				self.reply(self.tr(
					'other.docs',
					TextComponents.url(constants.DOCUMENTATION_URL, click=True).
					h(self.tr('other.docs.hover'))
				))

			elif self.what in self.COMMANDS_WITH_DETAILED_HELP:
				if not self.__has_permission(self.what):
					self.reply(self.tr('permission_denied', RText(self.what, RColor.gray)))
					return

				kwargs = {'prefix': self.__cmd_prefix}
				if self.what == 'crontab':
					kwargs['job_ids'] = ', '.join([f'{TextColors.job_id.mc_code}{jid.name}§r' for jid in CrontabJobId])
				elif self.what == 'database':
					name = mcdr_globals.metadata.name
					kwargs['name'] = name
					if self.config.database.compact.enabled:
						kwargs['scheduled_compact_notes'] = self.tr(
							f'node_help.{self.what}.scheduled_compact.on',
							name=name,
							cmd=f"§7{mkcmd(f'crontab {CrontabJobId.vacuum_sqlite.name}')}§r",
						)
					else:
						kwargs['scheduled_compact_notes'] = self.tr(f'node_help.{self.what}.scheduled_compact.off')
				elif self.what == 'export':
					from prime_backup.types.standalone_backup_format import StandaloneBackupFormat
					kwargs['export_formats'] = ', '.join([f'§3{ebf.name}§r' for ebf in StandaloneBackupFormat])
					kwargs['backup_meta_file_name'] = f'§3{constants.BACKUP_META_FILE_NAME}§r'

				self.__reply_help(self.tr(f'node_help.{self.what}', **kwargs))

			else:
				raise ValueError(self.what)
