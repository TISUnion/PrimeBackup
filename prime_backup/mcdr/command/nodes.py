import dataclasses
import json
import re
from typing import Optional, List

from mcdreforged.api.all import *
from typing_extensions import override

from prime_backup.utils import conversion_utils
from prime_backup.utils.backup_id_parser import BackupIdParser, BackupIdAlternatives
from prime_backup.utils.mcdr_utils import tr


class DateNode(ArgumentNode):
	@override
	def parse(self, text: str) -> ParseResult:
		result = QuotableText('temp').parse(text)
		try:
			ts = conversion_utils.date_to_timestamp_us(result.value.strip())
			return ParseResult(ts, result.char_read)
		except ValueError:
			raise IllegalArgument(tr('error.node.bad_date'), result.char_read)


class BackupIdNode(Text):
	@override
	def parse(self, text: str) -> ParseResult:
		result = super().parse(text)
		try:
			_ = BackupIdParser(allow_db_access=True, dry_run=True).parse(result.value)
		except ValueError:
			raise IllegalArgument(tr('error.node.bad_backup_id'), result.char_read)
		return result

	@classmethod
	def get_command_suggestions(cls) -> List[str]:
		return [
			*[bla.name for bla in BackupIdAlternatives],
			'~',
		]


class MultiBackupIdNode(BackupIdNode):
	@override
	def _on_visited(self, context: CommandContext, parsed_result: ParseResult):
		key = self.get_name()
		context[key] = context.get(key, []) + [parsed_result.value]


class IdRangeNode(ArgumentNode):
	@dataclasses.dataclass(frozen=True)
	class Range:
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

	@override
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


class HexStringNode(Text):
	__pattern = re.compile('[a-f0-9]+')

	@override
	def parse(self, text: str) -> ParseResult:
		result = super().parse(text)
		h: str = result.value.lower()
		if not self.__pattern.fullmatch(h):
			raise IllegalArgument(tr('error.node.bad_hex_string'), result.char_read)
		return ParseResult(h, result.char_read)


class InvalidJson(IllegalArgument):
	pass


class JsonObjectNode(ArgumentNode):
	@override
	def parse(self, text: str) -> ParseResult:
		if len(text) == 0:
			raise InvalidJson(tr('error.node.invalid_json.empty'), 0)
		if text[0] != '{':
			raise InvalidJson(tr('error.node.invalid_json.prefix'), 1)

		in_string = False
		is_escape = False
		level = 0
		for i, c in enumerate(text):
			if in_string:
				if is_escape:
					is_escape = False
				elif c == '\\':
					is_escape = True
				elif c == '"':
					in_string = False
			else:
				if c == '"':
					in_string = True
				elif c == '{':
					level += 1
				elif c == '}':
					level -= 1
					if level == 0:
						n = i + 1
						try:
							data = json.loads(text[:n])
						except ValueError as e:
							raise InvalidJson(tr('error.node.invalid_json.value', e), n)
						else:
							return ParseResult(data, n)

		raise InvalidJson(tr('error.node.invalid_json.suffix'), len(text))
