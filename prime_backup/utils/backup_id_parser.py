import enum
import re

from prime_backup.action.list_backup_action import ListBackupIdAction
from prime_backup.types.backup_filter import BackupFilter


class BackupIdAlternatives(enum.Enum):
	latest = enum.auto()


class BackupIdParser:
	class DbAccessNotAllowed(ValueError):
		pass

	class OffsetBackupNotFound(ValueError):
		def __init__(self, input_str: str, offset: int):
			super().__init__(f'found no backup in the database for input {input_str!r}')
			self.input_str = input_str
			self.offset = offset

	__parse_backup_id_relative_pattern = re.compile(r'~(\d*)')

	def __init__(self, *, allow_db_access: bool, dry_run: bool = False):
		self.allow_db_access = allow_db_access
		self.dry_run = dry_run

	def __get_nth_latest(self, s: str, offset: int, include_temp: bool) -> int:
		if not self.allow_db_access:
			raise self.DbAccessNotAllowed(f'db access not allowed')

		backup_filter = BackupFilter()
		if not include_temp:
			backup_filter.requires_non_temporary_backup()
		if self.dry_run:
			return 0

		candidates = ListBackupIdAction(backup_filter=backup_filter, offset=offset, limit=1).run()
		if len(candidates) == 0:
			raise self.OffsetBackupNotFound(s, offset)

		return candidates[0]

	def parse(self, s: str) -> int:
		"""
		Accept these following syntaxes:
		1. numbers: 1, 23, 456
		2. special strings: see BackupIdAlternatives, case-insensitive
		3. relative patterns: ~, ~1 ~3

		:raise: ValueError
		"""
		try:
			backup_id = int(s)
		except ValueError:
			pass
		else:
			if backup_id <= 0:
				raise ValueError(f'backup id should be greater than 0, found {backup_id}')
			return backup_id

		try:
			alt = BackupIdAlternatives[s.lower()]
		except KeyError:
			pass
		else:
			if alt == BackupIdAlternatives.latest:
				return self.__get_nth_latest(s, 0, False)

		if m := self.__parse_backup_id_relative_pattern.fullmatch(s):
			delta = int(m.group(1) or '0')
			return self.__get_nth_latest(s, delta, False)

		raise ValueError(f'bad backup id {s!r}')
