from typing import Dict, Callable, Any

from sqlalchemy import Engine, Inspector
from sqlalchemy.orm import Session

from xbackup.db import schema, db_logger


class DbMigration:
	DB_VERSION: int = 1

	def __init__(self, engine: Engine):
		self.logger = db_logger.get_logger()
		self.engine = engine
		self.migrations: Dict[int, Callable[[], Any]] = {
			2: self.__migrate_2,  # 1 -> 2
		}

	def migrate(self):
		inspector = Inspector.from_engine(self.engine)
		if inspector.has_table(schema.DbVersion.__tablename__):
			with Session(self.engine) as session:
				dbv = session.query(schema.DbVersion).first()
				if dbv is None:
					raise ValueError('table DbVersion is empty')
				current_version = dbv.version
				target_version = self.DB_VERSION

				if current_version != target_version:
					self.logger.info('DB migration starts. current db version: {}, target version: {}'.format(current_version, target_version))
					for i in range(current_version, target_version):
						self.logger.info('Migrating from v{} to v{}'.format(i, i + 1))
						self.migrations[i + 1]()

				dbv.version = target_version
		else:
			self.logger.info('Table {} does not exist, assuming newly create db, create everything'.format(schema.DbVersion.__tablename__))
			self.__create_the_world()
			pass

	def __create_the_world(self):
		schema.Base.metadata.create_all(self.engine)
		with Session(self.engine) as session, session.begin():
			session.add(schema.DbVersion(version=self.DB_VERSION))

	def __migrate_2(self):
		# noop for now, cuz we're still at version 1
		pass
