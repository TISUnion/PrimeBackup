import re
from typing import Optional

from mcdreforged.api.all import *

from prime_backup import constants
from prime_backup.mcdr import mcdr_globals
from prime_backup.mcdr.task import ImmediateTask


class ShowHelpTask(ImmediateTask):
	def __init__(self, source: CommandSource, full: bool, what: Optional[str] = None):
		super().__init__(source)
		self.full = full
		self.what = what

	@property
	def name(self) -> str:
		return 'help'

	@property
	def __cmd_prefix(self) -> str:
		return self.config.command.prefix

	def __reply_help(self, msg: RTextBase, hide_for_permission: bool = False):
		for line in msg.to_plain_text().splitlines():
			if hide_for_permission:
				match = re.match(r'(§7){} (\w+)([§ ])'.format(self.__cmd_prefix), line)
				if match is not None:
					literal = match.group(2)
					level = self.config.command.permission.get(literal, constants.DEFAULT_COMMAND_PERMISSION_LEVEL)
					if not self.source.has_permission(level):
						continue

			prefix = re.search(r'(?<=§7){}[-\w ]*(?=§)'.format(self.__cmd_prefix), line)
			if prefix is not None:
				self.reply(RText(line).set_click_event(RAction.suggest_command, prefix.group()), with_prefix=False)
			else:
				self.reply(line, with_prefix=False)

	def run(self) -> None:
		with self.source.preferred_language_context():
			if self.full:
				self.__reply_help(self.tr('help._header', name=mcdr_globals.metadata.name, version=mcdr_globals.metadata.version, description=mcdr_globals.metadata.get_description_rtext()), )

			if self.what is None:
				self.__reply_help(self.tr('help', prefix=self.__cmd_prefix), True)
			else:
				self.__reply_help(self.tr(f'help.{self.what}', prefix=self.__cmd_prefix))

