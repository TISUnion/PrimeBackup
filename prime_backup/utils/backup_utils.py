import dataclasses
import re
from typing import Optional, Tuple

_PATTERN_WORDS = re.compile(r'\w+')
_PATTERN_EXTRACT = re.compile(r'__pb_translated__:(\w+)')
_PATTERN_EXTRACT_WITH_ARGS = re.compile(r'__pb_translated__:(\w+):(.*)')


def create_translated_backup_comment(key: str, *args) -> str:
	if not _PATTERN_WORDS.fullmatch(key):
		raise ValueError(key)
	comment = f'__pb_translated__:{key}'
	if len(args) > 0:
		comment += ':' + '|'.join(map(str, args))
	return comment


@dataclasses.dataclass(frozen=True)
class ExtractResult:
	key: str
	args: Tuple[str, ...]


def extract_backup_comment_translation_key(comment: str) -> Optional[ExtractResult]:
	if (match := _PATTERN_EXTRACT.fullmatch(comment)) is not None:
		return ExtractResult(match.group(1), ())
	if (match := _PATTERN_EXTRACT_WITH_ARGS.fullmatch(comment)) is not None:
		return ExtractResult(match.group(1), tuple(match.group(2).split('|')))
	return None
