import contextlib
from pathlib import Path
from typing import Optional, Generator, Callable, TYPE_CHECKING, TypeVar

from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import Session

from prime_backup.config.config import Config
from prime_backup.db import db_constants
from prime_backup.db.migration import DbMigration
from prime_backup.db.session import DbSession

if TYPE_CHECKING:
	from prime_backup.types.hash_method import HashMethod, Hasher


_T = TypeVar('_T')


class _HashMethodCache:
	def __init__(self, hash_method: 'HashMethod'):
		from prime_backup.types.hash_method import HashMethod
		if not isinstance(hash_method, HashMethod):
			raise TypeError('hash_method must be a HashMethod, got {!r}'.format(type(hash_method)))

		self.hash_method: 'HashMethod' = hash_method
		self.create_hasher_func: Callable[[], 'Hasher'] = hash_method.value.create_hasher  # cache this


class DbAccess:
	__engine: Optional[Engine] = None
	__db_file_path: Optional[Path] = None

	__hash_method_cache: Optional[_HashMethodCache] = None

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

		cls.sync_hash_method()

	@classmethod
	def init_memory_db(cls):
		cls.__engine = create_engine('sqlite://')
		cls.__db_file_path = None

		try:
			DbMigration.create_the_world(cls.__engine)
		except Exception:
			cls.__engine = None
			raise

		cls.sync_hash_method()

	@classmethod
	def shutdown(cls):
		if (engine := cls.__engine) is not None:
			engine.dispose()
			cls.__engine = None
		cls.__hash_method_cache = None
		cls.__db_file_path = None

	@classmethod
	def _set_hash_method(cls, hash_method: 'HashMethod'):
		cls.__hash_method_cache = _HashMethodCache(hash_method)

	@classmethod
	def sync_hash_method(cls):
		with cls.open_session() as session:
			hash_method_str = str(session.get_db_meta().hash_method)
		from prime_backup.types.hash_method import HashMethod
		try:
			cls.__hash_method_cache = _HashMethodCache(HashMethod[hash_method_str])
		except KeyError:
			raise ValueError('invalid hash method {!r} in db meta'.format(hash_method_str)) from None

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
	def get_hash_method(cls) -> 'HashMethod':
		return cls.__ensure_not_none(cls.__hash_method_cache).hash_method

	@classmethod
	def _get_hash_method_no_check(cls) -> 'HashMethod':
		# faster than get_hash_method(), useful for hot paths
		assert cls.__hash_method_cache is not None
		return cls.__hash_method_cache.hash_method

	@classmethod
	def _create_hasher_no_check(cls) -> 'Hasher':
		assert cls.__hash_method_cache is not None
		return cls.__hash_method_cache.create_hasher_func()

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
