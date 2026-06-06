import contextlib
from pathlib import Path
from typing import Optional, Generator, TypeVar

from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import Session

from prime_backup.config.config import Config
from prime_backup.db import db_constants
from prime_backup.db.db_meta_cache import DbMetaCache
from prime_backup.db.migration import DbMigration
from prime_backup.db.session import DbSession

_T = TypeVar('_T')


class DbAccess:
	__engine: Optional[Engine] = None
	__db_file_path: Optional[Path] = None

	@classmethod
	def init(cls, create: bool, migrate: bool):
		"""
		"""
		config = Config.get()
		db_dir = config.storage_path
		if create:
			db_dir.mkdir(parents=True, exist_ok=True)

		db_path = db_dir / db_constants.DB_FILE_NAME
		cls.__engine = create_engine('sqlite:///' + str(db_path))
		cls.__db_file_path = db_path

		try:
			migration = DbMigration(cls.__engine, db_dir, db_path, config.temp_path)
			migration.check_and_migrate(create=create, migrate=migrate)
		except Exception:
			cls.__engine = None
			cls.__db_file_path = None
			raise

		cls.sync_meta_cache()

	@classmethod
	def init_memory_db(cls):
		cls.__engine = create_engine('sqlite://')
		cls.__db_file_path = None

		try:
			DbMigration.create_the_world(cls.__engine)
		except Exception:
			cls.__engine = None
			raise

		cls.sync_meta_cache()

	@classmethod
	def shutdown(cls):
		if (engine := cls.__engine) is not None:
			engine.dispose()
			cls.__engine = None
		DbMetaCache.reset()
		cls.__db_file_path = None

	@classmethod
	def sync_meta_cache(cls):
		from prime_backup.types.db_meta_info import DbMetaInfo
		with cls.open_session() as session:
			DbMetaCache.set(DbMetaInfo.of(session.get_db_meta()))

	@classmethod
	def __ensure_engine(cls) -> Engine:
		if cls.__engine is None:
			raise RuntimeError('engine unavailable')
		return cls.__engine

	@classmethod
	def __ensure_not_none(cls, value: Optional[_T]) -> _T:
		if value is None:
			raise RuntimeError('db not is not initialized yet')
		return value

	@classmethod
	def is_initialized(cls) -> bool:
		return cls.__engine is not None

	@classmethod
	def get_db_file_path(cls) -> Path:
		return cls.__ensure_not_none(cls.__db_file_path)

	@classmethod
	@contextlib.contextmanager
	def open_session(cls) -> Generator['DbSession', None, None]:
		with Session(cls.__ensure_engine()) as session, session.begin():
			yield DbSession(session, cls.__db_file_path)

	@classmethod
	@contextlib.contextmanager
	def enable_echo(cls) -> Generator[None, None, None]:
		engine = cls.__ensure_engine()
		engine.echo = True
		try:
			yield
		finally:
			engine.echo = False
