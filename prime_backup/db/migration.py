from typing import Dict, Callable, Any, Optional

from sqlalchemy import Engine, Inspector, select
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

					self.__migrate_db(session, dbm, current_version, target_version)
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

	def __migrate_db(self, session: Session, dbm: schema.DbMeta, current_version, target_version):
		self.logger.info('DB migration starts. current DB version: {}, target version: {}'.format(current_version, target_version))

		for i in range(current_version, target_version):
			self.logger.info('Migrating database from version {} to version {}'.format(i, i + 1))
			self.migrations[i + 1](session)
		dbm.version = target_version

		self.logger.info('DB migration done, new db version: {}'.format(target_version))

	def __migrate_1_2(self, session: Session):
		"""
		v1.7.0 changes: renamed backup tag "pre_restore_backup" to tag "temporary"
		"""
		from prime_backup.types.backup_tags import BackupTagName

		backups = session.execute(select(schema.Backup)).scalars().all()
		src_tag = 'pre_restore_backup'
		dst_tag = BackupTagName.temporary.name
		for backup in backups:
			tags = dict(backup.tags)
			if src_tag in tags:
				tags[dst_tag] = tags.pop(src_tag)
				backup.tags = tags
				self.logger.info('Renaming tag {!r} to {!r} for backup #{}, new tags: {}'.format(
					src_tag, dst_tag, backup.id, backup.tags,
				))
