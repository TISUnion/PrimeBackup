from typing import Dict, Callable, Any, Optional

from sqlalchemy import Engine, Inspector
from sqlalchemy.orm import Session

from xbackup.config.config import Config
from xbackup.db import schema, db_logger


class DbMigration:
	DB_MAGIC_INDEX: int = 0
	DB_VERSION: int = 1

	def __init__(self, engine: Engine):
		self.config = Config.get()
		self.logger = db_logger.get_logger()
		self.engine = engine
		self.migrations: Dict[int, Callable[[], Any]] = {
			2: self.__migrate_2,  # 1 -> 2
		}

	def migrate(self):
		inspector = Inspector.from_engine(self.engine)
		if inspector.has_table(schema.DbMeta.__tablename__):
			with Session(self.engine) as session:
				dbm: Optional[schema.DbMeta] = session.get(schema.DbMeta, self.DB_MAGIC_INDEX)
				if dbm is None:
					raise ValueError('table DbMeta is empty')

				self.__check_db_meta(dbm)

				current_version = dbm.version
				target_version = self.DB_VERSION

				if current_version != target_version:
					self.logger.info('DB migration starts. current db version: {}, target version: {}'.format(current_version, target_version))
					for i in range(current_version, target_version):
						self.logger.info('Migrating from v{} to v{}'.format(i, i + 1))
						self.migrations[i + 1]()

				dbm.version = target_version
		else:
			self.logger.info('Table {} does not exist, assuming newly create db, create everything'.format(schema.DbMeta.__tablename__))
			self.__create_the_world()
			pass

	@property
	def __configured_hash_method(self) -> str:
		return self.config.backup.hash_method.name

	def __create_the_world(self):
		schema.Base.metadata.create_all(self.engine)
		with Session(self.engine) as session, session.begin():
			session.add(schema.DbMeta(
				magic=self.DB_MAGIC_INDEX,
				version=self.DB_VERSION,
				hash_method=self.__configured_hash_method,
			))

	def __check_db_meta(self, dbm: schema.DbMeta):
		if dbm.hash_method != self.__configured_hash_method:
			raise ValueError('hash method mismatch, {} is used in this database, but {} is configured to used'.format(dbm.hash_method, self.__configured_hash_method))

	def __migrate_2(self):
		# noop for now, cuz we're still at version 1
		pass
