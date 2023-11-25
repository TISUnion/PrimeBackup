import contextlib
import sys
from typing import Optional, ContextManager

from sqlalchemy import create_engine, Engine, text
from sqlalchemy.orm import Session

from xbackup.config.config import Config
from xbackup.db import db_logger
from xbackup.db.migration import DbMigration
from xbackup.db.session import DbSession


class DbAccess:
	__engine: Optional[Engine] = None
	__max_variable_number: int = 999  # the limit in old sqlite (https://www.sqlite.org/limits.html#max_variable_number)

	@classmethod
	def init(cls):
		db_dir = Config.get().storage_path
		db_dir.mkdir(parents=True, exist_ok=True)
		db_logger.init_logger(db_dir)

		db_path = db_dir / 'xbackup.db'
		cls.__engine = create_engine('sqlite:///' + str(db_path))
		cls.__max_variable_number = cls.__get_var_limit()

		DbMigration(cls.__engine).migrate()

	@classmethod
	@contextlib.contextmanager
	def open_session(cls) -> ContextManager['DbSession']:
		with Session(cls.__engine) as session, session.begin():
			yield DbSession(session, cls.__max_variable_number)

	@classmethod
	def __get_var_limit(cls) -> int:
		with Session(cls.__engine) as session:
			for line in session.execute(text('PRAGMA compile_options;')).scalars().all():
				if line.startswith('MAX_VARIABLE_NUMBER=') or line.startswith('SQLITE_LIMIT_VARIABLE_NUMBER='):
					db_logger.get_logger().info(line)
					return int(line.split('=', 1)[1])
			else:
				print('max var num not found', file=sys.stderr)
				return 999
				raise KeyError('not found')

	@classmethod
	@contextlib.contextmanager
	def enable_echo(cls) -> ContextManager[None]:
		cls.__engine.echo = True
		try:
			yield
		finally:
			cls.__engine.echo = False
