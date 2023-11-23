import contextlib
from typing import Optional, ContextManager

from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import Session

from xbackup.config.config import Config
from xbackup.db import db_logger
from xbackup.db.migration import DbMigration
from xbackup.db.session import DbSession


class DbAccess:
	__engine: Optional[Engine] = None

	@classmethod
	def init(cls):
		db_dir = Config.get().storage_path
		db_dir.mkdir(parents=True, exist_ok=True)
		db_path = db_dir / 'xbackup.db'
		cls.__engine = create_engine('sqlite:///' + str(db_path))

		db_logger.init_logger(db_dir)

		DbMigration(cls.__engine).migrate()

	@classmethod
	@contextlib.contextmanager
	def open_session(cls) -> ContextManager['DbSession']:
		with Session(cls.__engine) as session, session.begin():
			yield DbSession(session)
