import sqlite3

from prime_backup.utils.path_like import PathLike


def get_sqlite_version() -> str:
	return sqlite3.sqlite_version


def check_sqlite_json_query_support() -> bool:
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


def check_sqlite_vacuum_into_support() -> bool:
	return sqlite3.sqlite_version_info >= (3, 27, 0)


def check_sqlite_row_number() -> bool:
	return sqlite3.sqlite_version_info >= (3, 25, 0)


def vacuum_into_via_backup_api(src_db_path: PathLike, into_path: PathLike):
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


if __name__ == '__main__':
	print('version:', sqlite3.sqlite_version)
	print('json query:', check_sqlite_json_query_support())
	print('vacuum into:', check_sqlite_vacuum_into_support())
	print('row number:', check_sqlite_row_number())
