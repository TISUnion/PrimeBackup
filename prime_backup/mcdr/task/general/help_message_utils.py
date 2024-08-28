import dataclasses
import re
from typing import List, Optional

from mcdreforged.api.all import *

from prime_backup.config.config import Config


@dataclasses.dataclass(frozen=True)
class HelpMessageLine:
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
		suggest_match = re.search(r'(?<=§7){}[-\w ]*(?=[§\[<])'.format(prefix), line)
		suggest = suggest_match.group() if suggest_match is not None else None

		text = RText(line)
		if suggest is not None:
			text.c(RAction.suggest_command, suggest)

		cmd_match = re.match(r'(§7){} (\w+)([§ ])'.format(prefix), line)
		if cmd_match is not None:
			literal = cmd_match.group(2)
			permission = config.command.permission.get(literal)
		elif line.startswith(f'§7{prefix}§r'):  # root node
			literal = ''
			permission = 0
		else:
			result.append(HelpMessageLine(line, text, None, None, suggest))
			continue

		result.append(HelpMessageLine(
			line=line,
			text=text,
			literal=literal,
			permission=permission,
			suggest=suggest,
		))
	return result
