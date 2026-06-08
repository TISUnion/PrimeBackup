import functools
import sqlite3

from typing import Callable

from prime_backup.utils import db_utils


class DbFeatures:
	@classmethod
	def __check_support(cls, check_func: Callable[[], bool], msg: str) -> bool:
		if not (is_supported := check_func()):
			from prime_backup import logger
			logger.get().warning(f'WARN: {msg}. SQLite version: {db_utils.get_sqlite_version()}')
		return is_supported

	@classmethod
	@functools.lru_cache(None)
	def supports_json_query(cls) -> bool:
		"""
		https://sqlite.org/json1.html#compiling_in_json_support
		A simple version check might not work, so here's a test
		"""
		def do_check() -> bool:
			try:
				with sqlite3.connect(':memory:') as conn:
					cursor = conn.cursor()
					cursor.execute('SELECT JSON(\'{"a": "b"}\')')
					cursor.fetchone()
					cursor.close()
			except sqlite3.OperationalError:
				return False
			else:
				return True

		return cls.__check_support(do_check, 'SQLite backend does not support json query. Inefficient manual query is used as the fallback')

	@classmethod
	@functools.lru_cache(None)
	def supports_vacuum_into(cls) -> bool:
		"""
		https://sqlite.org/releaselog/3_27_0.html
		"""
		return cls.__check_support(
			lambda: sqlite3.sqlite_version_info >= (3, 27, 0),
			'SQLite backend does not support VACUUM INTO statement. Insecure manual file copy is used as the fallback',
		)

	@classmethod
	@functools.lru_cache(None)
	def supports_row_number(cls) -> bool:
		"""
		https://sqlite.org/windowfunctions.html#history
		"""
		return cls.__check_support(
			lambda: sqlite3.sqlite_version_info >= (3, 25, 0),
			'SQLite backend does not support ROW_NUMBER() statement, ID reassignment is not available',
		)

	@classmethod
	@functools.lru_cache(None)
	def supports_returning(cls) -> bool:
		"""
		https://sqlite.org/lang_returning.html#overview
		"""
		return sqlite3.sqlite_version_info >= (3, 35, 0)

	@classmethod
	@functools.lru_cache(None)
	def supports_without_rowid(cls) -> bool:
		"""
		https://sqlite.org/withoutrowid.html#compatibility
		"""
		return sqlite3.sqlite_version_info >= (3, 8, 2)

	@classmethod
	def _debug_print_states(cls):
		print('version:', db_utils.get_sqlite_version())
		print('json query:', cls.supports_json_query())
		print('vacuum into:', cls.supports_vacuum_into())
		print('returning:', cls.supports_returning())
		print('row number:', cls.supports_row_number())
		print('without rowid:', cls.supports_without_rowid())


if __name__ == '__main__':
	DbFeatures._debug_print_states()
