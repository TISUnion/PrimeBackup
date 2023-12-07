import re
from typing import NamedTuple, List, Optional

from mcdreforged.api.all import *

from prime_backup.config.config import Config


class HelpMessageLine(NamedTuple):
	line: str
	text: RTextBase
	literal: Optional[str]
	permission: Optional[int]
	suggest: Optional[str]

	def is_help(self) -> bool:
		return self.literal is not None and self.permission is not None and self.suggest is not None


def parse_help_message(msg: RTextBase) -> List[HelpMessageLine]:
	"""
	Notes: should be inside a CommandSource.preferred_language_context or whatever
	"""
	# hacky, but it works
	config = Config.get()
	prefix = config.command.prefix
	result = []
	for line in msg.to_plain_text().splitlines():
		match = re.match(r'(§7){} (\w+)([§ ])'.format(prefix), line)
		if match is not None:
			literal = match.group(2)
			permission = config.command.permission.get(literal)
		elif line.startswith(f'§7{prefix}§r'):  # root node
			literal = ''
			permission = 0
		else:
			result.append(HelpMessageLine(line, RText(line), None, None, None))
			continue

		suggest_match = re.search(r'(?<=§7){}[-\w ]*(?=[§\[<])'.format(prefix), line)
		if suggest_match is None:
			raise ValueError(line)
		suggest = suggest_match.group()

		result.append(HelpMessageLine(
			line=line,
			text=RText(line).c(RAction.suggest_command, suggest),
			literal=literal,
			permission=permission,
			suggest=suggest,
		))
	return result
