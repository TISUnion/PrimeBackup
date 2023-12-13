import sqlite3


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


if __name__ == '__main__':
	print(sqlite3.sqlite_version)
	print(check_sqlite_json_query_support())
