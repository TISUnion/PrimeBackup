import contextlib
from typing import Optional, ContextManager

from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import Session

from prime_backup.config.config import Config
from prime_backup.db import db_logger
from prime_backup.db.migration import DbMigration
from prime_backup.db.session import DbSession


class DbAccess:
	DB_FILE = 'prime_backup.db'
	__engine: Optional[Engine] = None

	@classmethod
	def init(cls, auto_migrate: bool = True):
		db_dir = Config.get().storage_path
		db_dir.mkdir(parents=True, exist_ok=True)
		db_logger.init_logger(db_dir)

		db_path = db_dir / cls.DB_FILE
		cls.__engine = create_engine('sqlite:///' + str(db_path))

		migration = DbMigration(cls.__engine)
		if auto_migrate:
			migration.migrate()
		else:
			migration.ensure_version()

	@classmethod
	def shutdown(cls):
		logger = db_logger.get()
		for hdr in list(logger.handlers):
			logger.removeHandler(hdr)

	@classmethod
	@contextlib.contextmanager
	def open_session(cls) -> ContextManager['DbSession']:
		with Session(cls.__engine) as session, session.begin():
			yield DbSession(session)

	@classmethod
	@contextlib.contextmanager
	def enable_echo(cls) -> ContextManager[None]:
		cls.__engine.echo = True
		try:
			yield
		finally:
			cls.__engine.echo = False
