import time
from pathlib import Path
from typing import Dict, Callable, Any, Optional

from sqlalchemy import Engine, Inspector, text
from sqlalchemy.orm import Session

from prime_backup import logger
from prime_backup.config.config import Config
from prime_backup.db import schema, db_constants
from prime_backup.db.db_file_backup import _DbFileBackupHelper
from prime_backup.exceptions import PrimeBackupError


class BadDbVersion(PrimeBackupError):
	pass


class DbMigration:
	DB_MAGIC_INDEX = db_constants.DB_MAGIC_INDEX
	DB_VERSION = db_constants.DB_VERSION

	def __init__(self, engine: Engine, db_dir: Path, db_file: Path):
		self.logger = logger.get()
		self.engine = engine
		self.db_dir = db_dir
		self.db_file = db_file
		self.migrations: Dict[int, Callable[[Session], Any]] = {
			2: self.__migrate_1_2,  # 1 -> 2
			3: self.__migrate_2_3,  # 2 -> 3
		}

	def check_and_migrate(self, *, create: bool, migrate: bool):
		inspector = Inspector.from_engine(self.engine)
		if inspector.has_table(schema.DbMeta.__tablename__):
			with Session(self.engine) as session, session.begin():
				dbm: Optional[schema.DbMeta] = session.get(schema.DbMeta, self.DB_MAGIC_INDEX)
				if dbm is None:
					raise ValueError('table DbMeta is empty')

				current_version = dbm.version
				target_version = self.DB_VERSION

			if current_version != target_version:
				if not migrate:
					raise BadDbVersion('DB version mismatch (expect {}, found {}), please migrate in the MCDR', self.DB_VERSION, dbm.version)

				if current_version > target_version:
					self.logger.error('The current DB version {} is larger than expected {}'.format(current_version, target_version))
					raise ValueError('existing db version {} too large'.format(current_version))

				self.__migrate_db(current_version, target_version)
		else:
			if not create:
				raise BadDbVersion('DbMeta table not found')

			self.logger.info('Table {} does not exist, assuming newly created db, create everything'.format(schema.DbMeta.__tablename__))
			self.__create_the_world()

	def __create_the_world(self):
		schema.Base.metadata.create_all(self.engine)
		config = Config.get()
		with Session(self.engine) as session, session.begin():
			session.add(schema.DbMeta(
				magic=self.DB_MAGIC_INDEX,
				version=self.DB_VERSION,
				hash_method=config.backup.hash_method.name,
			))

	def __migrate_db(self, current_version: int, target_version: int):
		start_ts = time.time()
		self.logger.info('DB migration starts. current DB version: {}, target version: {}'.format(current_version, target_version))

		backup_helper = _DbFileBackupHelper(self.db_file, self.db_dir / 'db_backup', 'pre_migration_{}to{}_{}'.format(current_version, target_version, time.strftime('%Y%m%d')), db_constants.DB_FILE_NAME)
		self.logger.info('Creating DB pre migration backup at {}'.format(str(backup_helper.backup_file)))
		backup_helper.create(skip_existing=True)

		try:
			def update_dbm_version(v: int):
				dbm: Optional[schema.DbMeta] = session.get(schema.DbMeta, self.DB_MAGIC_INDEX)
				if dbm is None:
					raise ValueError('table DbMeta is empty')
				dbm.version = v

			for i in range(current_version, target_version):
				next_version = i + 1
				self.logger.info('Migrating database from version {} to version {}'.format(i, next_version))
				with Session(self.engine) as session, session.begin():
					self.migrations[i + 1](session)
					update_dbm_version(next_version)

			with Session(self.engine) as session, session.begin():
				update_dbm_version(target_version)

			with Session(self.engine) as session, session.begin():
				session.execute(text('VACUUM'))
		except Exception:
			self.logger.error('DB migration failed, restoring pre migration backup {}'.format(str(backup_helper.backup_file)))
			try:
				backup_helper.restore()
			except Exception:
				self.logger.exception('Pre migration backup restored failed')
			raise

		self.logger.info('DB migration done, new db version: {}, total cost {:.1f}s'.format(target_version, time.time() - start_ts))

	def __migrate_1_2(self, session: Session):
		"""
		v1.7.0 changes: renamed backup tag "pre_restore_backup" to tag "temporary"
		"""
		from prime_backup.db.migrations.migration_1_2 import MigrationImpl1To2
		MigrationImpl1To2(self.engine, session).migrate()

	def __migrate_2_3(self, session: Session):
		"""
		v1.9.0 changes: fileset-based file reusing
		"""
		from prime_backup.db.migrations.migration_2_3 import MigrationImpl2To3
		MigrationImpl2To3(self.engine, session).migrate()
