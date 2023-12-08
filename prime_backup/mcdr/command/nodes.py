import re
from typing import NamedTuple, Optional

from mcdreforged.api.all import *

from prime_backup.utils import conversion_utils
from prime_backup.utils.mcdr_utils import tr


class DateNode(ArgumentNode):
	def parse(self, text: str) -> ParseResult:
		result = QuotableText('temp').parse(text)
		try:
			ts = conversion_utils.date_to_timestamp_ns(result.value.strip())
			return ParseResult(ts, result.char_read)
		except ValueError:
			raise IllegalArgument(tr('error.node.bad_date'), result.char_read)


class MultiIntegerNode(Integer):
	def _on_visited(self, context: CommandContext, parsed_result: ParseResult):
		if self.get_name() not in context:
			context[self.get_name()] = []
		context[self.get_name()].append(parsed_result.value)


class IdRangeNode(ArgumentNode):
	class Range(NamedTuple):
		start: Optional[int]
		end: Optional[int]

	__patterns = list(map(re.compile, [
		r'\*',
		r'(?P<start>\d+)-',
		r'(?P<start>\d+)~',
		r'(?P<start>\d+)-(?P<end>\d+)',
		r'(?P<start>\d+)~(?P<end>\d+)',
		r'-(?P<end>\d+)',
		r'~(?P<end>\d+)',
		r'\[ *(?P<start>\d+) *, *(?P<end>\d+) *\]',
	]))

	def parse(self, text: str) -> ParseResult:
		result = QuotableText('temp').parse(text)
		text: str = result.value.strip()
		for pattern in self.__patterns:
			if (match := pattern.fullmatch(text)) is not None:
				groups = match.groupdict()
				if (start := groups.get('start')) is not None:
					start = int(start)
				if (end := groups.get('end')) is not None:
					end = int(end)
				r = self.Range(start, end)
				break
		else:
			raise IllegalArgument(tr('error.node.bad_id_range'), result.char_read)
		return ParseResult(r, result.char_read)
