from typing import Optional, Union

from mcdreforged.api.all import *

from prime_backup.mcdr.task.basic_task import ImmediateTask
from prime_backup.mcdr.task.general import help_message_utils


class ShowHelpTask(ImmediateTask):
	def __init__(self, source: CommandSource, what: Optional[str] = None):
		super().__init__(source)
		self.what = what

	@property
	def name(self) -> str:
		return 'help'

	@property
	def __cmd_prefix(self) -> str:
		return self.config.command.prefix

	def reply(self, msg: Union[str, RTextBase], *, with_prefix: bool = False):
		super().reply(msg, with_prefix=with_prefix)

	def __reply_help(self, msg: RTextBase, hide_for_permission: bool = False):
		for h in help_message_utils.parse_help_message(msg):
			if hide_for_permission and h.is_help() and not self.source.has_permission(h.permission):
				continue
			self.reply(h.text)

	def run(self) -> None:
		with self.source.preferred_language_context():
			if self.what is None:
				from prime_backup.mcdr.crontab_job import CrontabJobId
				from prime_backup.types.standalone_backup_format import StandaloneBackupFormat
				t_export_formats = ', '.join([f'§3{ebf.name}§r' for ebf in StandaloneBackupFormat])
				t_job_ids = ', '.join([f'§5{jid.name}§r' for jid in CrontabJobId])
				self.reply(self.tr('commands.title').set_color(RColor.light_purple))
				self.__reply_help(self.tr('commands.content', prefix=self.__cmd_prefix), True)
				self.reply(self.tr('arguments.title').set_color(RColor.light_purple))
				self.__reply_help(self.tr('arguments.content', export_formats=t_export_formats, job_ids=t_job_ids))
			else:
				self.__reply_help(self.tr(f'node_help.{self.what}', prefix=self.__cmd_prefix))

