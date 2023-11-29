import re
from typing import Optional

from mcdreforged.api.all import *

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

	def __splitted_reply(self, msg: RTextBase):
		for line in msg.to_plain_text().splitlines():
			prefix = re.search(r'(?<=§7){}[\w ]*(?=§)'.format(self.config.command.prefix), line)
			if prefix is not None:
				self.reply(RText(line).set_click_event(RAction.suggest_command, prefix.group()), with_prefix=False)
			else:
				self.reply(line, with_prefix=False)

	def run(self) -> None:
		with self.source.preferred_language_context():
			if self.full:
				self.__splitted_reply(self.tr('help._header', name=mcdr_globals.metadata.name, version=mcdr_globals.metadata.version, description=mcdr_globals.metadata.get_description_rtext()))

			if self.what is None:
				msg = self.tr('help', prefix=self.config.command.prefix)
			else:
				msg = self.tr(f'help.{self.what}', prefix=self.config.command.prefix)
			self.__splitted_reply(msg)

