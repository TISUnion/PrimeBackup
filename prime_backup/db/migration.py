from typing import Dict, Callable, Any, Optional

from sqlalchemy import Engine, Inspector, text
from sqlalchemy.orm import Session

from prime_backup import logger
from prime_backup.config.config import Config
from prime_backup.db import schema, db_constants
from prime_backup.exceptions import PrimeBackupError


class BadDbVersion(PrimeBackupError):
	pass


class DbMigration:
	DB_MAGIC_INDEX = db_constants.DB_MAGIC_INDEX
	DB_VERSION = db_constants.DB_VERSION

	def __init__(self, engine: Engine):
		self.logger = logger.get()
		self.engine = engine
		self.migrations: Dict[int, Callable[[Session], Any]] = {
			2: self.__migrate_1_2,  # 1 -> 2
		}

	def check_and_migrate(self):
		inspector = Inspector.from_engine(self.engine)
		if inspector.has_table(schema.DbMeta.__tablename__):
			with Session(self.engine) as session, session.begin():
				dbm: Optional[schema.DbMeta] = session.get(schema.DbMeta, self.DB_MAGIC_INDEX)
				if dbm is None:
					raise ValueError('table DbMeta is empty')

				self.__check_db_meta(dbm)

				current_version = dbm.version
				target_version = self.DB_VERSION

				if current_version != target_version:
					if current_version > target_version:
						self.logger.error('The current DB version {} is larger than expected {}'.format(current_version, target_version))
						raise ValueError('existing db version {} too large'.format(current_version))

					self.logger.info('DB migration starts. current DB version: {}, target version: {}'.format(current_version, target_version))
					for i in range(current_version, target_version):
						self.logger.info('Migrating from v{} to v{}'.format(i, i + 1))
						self.migrations[i + 1](session)
					dbm.version = target_version
					self.logger.info('DB migration done')
		else:
			self.logger.info('Table {} does not exist, assuming newly created db, create everything'.format(schema.DbMeta.__tablename__))
			self.__create_the_world()
			pass

	def ensure_version(self):
		"""
		Minimum check
		"""
		inspector = Inspector.from_engine(self.engine)
		if inspector.has_table(schema.DbMeta.__tablename__):
			with Session(self.engine) as session:
				dbm = session.get(schema.DbMeta, self.DB_MAGIC_INDEX)
				if dbm is not None:
					if dbm.version == self.DB_VERSION:
						return
					else:
						raise BadDbVersion('DB version mismatch (expect {}, found {}), please migrate in the MCDR', self.DB_VERSION, dbm.version)
				else:
					raise BadDbVersion('Bad DbMeta table')
		raise BadDbVersion('DbMeta table not found')

	def __create_the_world(self):
		schema.Base.metadata.create_all(self.engine)
		config = Config.get()
		with Session(self.engine) as session, session.begin():
			session.add(schema.DbMeta(
				magic=self.DB_MAGIC_INDEX,
				version=self.DB_VERSION,
				hash_method=config.backup.hash_method.name,
			))

	def __check_db_meta(self, dbm: schema.DbMeta):
		pass

	def __migrate_1_2(self, session: Session):
		# noinspection PyUnreachableCode
		if False:
			table = schema.Backup.__tablename__
			column = schema.Backup.timestamp
			name = column.key
			type_ = column.type.compile(dialect=self.engine.dialect)
			self.logger.info('Adding column {!r} ({}) to table {!r}'.format(name, type_, table))
			session.execute(text(f'ALTER TABLE {table} ADD COLUMN {name} {type_}'))
