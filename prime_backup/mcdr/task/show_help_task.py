import re

from mcdreforged.api.all import *

from prime_backup.mcdr import mcdr_globals
from prime_backup.mcdr.task import ImmediateTask


class ShowHelpTask(ImmediateTask):
	@property
	def name(self) -> str:
		return 'help'

	def run(self) -> None:
		with self.source.preferred_language_context():
			msg = self.tr('help', self.config.command.prefix, mcdr_globals.metadata.name, mcdr_globals.metadata.version, mcdr_globals.metadata.get_description_rtext())
			for line in msg.to_plain_text().splitlines():
				prefix = re.search(r'(?<=§7){}[\w ]*(?=§)'.format(self.config.command.prefix), line)
				if prefix is not None:
					self.reply(RText(line).set_click_event(RAction.suggest_command, prefix.group()), with_prefix=False)
				else:
					self.reply(line, with_prefix=False)
