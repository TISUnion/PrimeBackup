import sqlite3


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


if __name__ == '__main__':
	print('version:', sqlite3.sqlite_version)
	print('json query:', check_sqlite_json_query_support())
	print('vacuum into:', check_sqlite_vacuum_into_support())
	print('row number:', check_sqlite_row_number())
