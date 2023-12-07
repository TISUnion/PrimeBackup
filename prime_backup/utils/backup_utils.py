import re
from typing import Optional


_PATTERN_WORDS = re.compile(r'\w+')
_PATTERN_EXTRACT = re.compile(r'__pb_translated__:(\w+)')


def create_translated_backup_comment(key: str):
	if not _PATTERN_WORDS.fullmatch(key):
		raise ValueError(key)
	return f'__pb_translated__:{key}'


def extract_backup_comment_translation_key(comment: str) -> Optional[None]:
	if (match := _PATTERN_EXTRACT.fullmatch(comment)) is not None:
		return match.group(1)
	return None
