import sqlite3

from typing import TYPE_CHECKING

if TYPE_CHECKING:
	from prime_backup.utils.path_like import PathLike


def get_sqlite_version() -> str:
	return sqlite3.sqlite_version


def vacuum_into_via_backup_api(src_db_path: 'PathLike', into_path: 'PathLike'):
	"""
	Fallback for VACUUM INTO on old SQLite versions that do not support "VACUUM INTO"
	"""
	src_conn = sqlite3.connect(src_db_path, timeout=30)
	dest_conn = sqlite3.connect(into_path, timeout=30)
	try:
		src_conn.backup(dest_conn)
		dest_conn.execute('VACUUM')
		dest_conn.commit()
	finally:
		dest_conn.close()
		src_conn.close()
