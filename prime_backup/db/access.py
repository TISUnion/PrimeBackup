import contextlib
from pathlib import Path
from typing import Optional, ContextManager

from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import Session

from prime_backup.config.config import Config
from prime_backup.db import db_logger, db_constants
from prime_backup.db.migration import DbMigration
from prime_backup.db.session import DbSession
from prime_backup.types.hash_method import HashMethod


class DbAccess:
	__engine: Optional[Engine] = None
	__db_path: Optional[Path] = None
	__hash_method: Optional[HashMethod] = None

	@classmethod
	def init(cls, auto_migrate: bool = True):
		"""
		:param auto_migrate:
			True: check db meta, try to migrate;
			False: check db version only
		"""
		db_dir = Config.get().storage_path
		db_dir.mkdir(parents=True, exist_ok=True)
		db_logger.init_logger()

		db_path = db_dir / db_constants.DB_FILE_NAME
		cls.__engine = create_engine('sqlite:///' + str(db_path))
		cls.__db_path = db_path

		migration = DbMigration(cls.__engine)
		if auto_migrate:
			migration.check_and_migrate()
		else:
			migration.ensure_version()

		with cls.open_session() as session:
			hash_method_str = str(session.get_db_meta().hash_method)
		try:
			cls.__hash_method = HashMethod[hash_method_str]
		except KeyError:
			raise ValueError('invalid hash method {!r} in db meta'.format(hash_method_str)) from None

	@classmethod
	def shutdown(cls):
		if (logger := db_logger.get()) is not None:
			for hdr in list(logger.handlers):
				logger.removeHandler(hdr)

	@classmethod
	def __ensure_not_none(cls, value):
		if value is None:
			raise RuntimeError('db not is not initialized yet')
		return value

	@classmethod
	def get_db_path(cls) -> Path:
		return cls.__ensure_not_none(cls.__db_path)

	@classmethod
	def get_hash_method(cls) -> HashMethod:
		return cls.__ensure_not_none(cls.__hash_method)

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
