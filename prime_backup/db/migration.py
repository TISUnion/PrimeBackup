import time
from typing import Dict, Callable, Any, Optional, List

from sqlalchemy import Engine, Inspector, select, text
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

					self.__migrate_db(session, dbm, current_version, target_version)
					has_migration = True
				else:
					has_migration = False

			if has_migration:
				self.logger.info('Migration done, performing VACUUM to tidy up')
				with Session(self.engine) as session, session.begin():
					session.execute(text('VACUUM'))
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

	def __migrate_2_3(self, session: Session):
		"""
		v1.8.0 changes: file trim
		"""
		from prime_backup.db.session import DbSession
		db_session = DbSession(session)

		self.logger.info('Relocating the old "file" table')
		session.execute(text('CREATE TABLE old_file AS SELECT * FROM file'))
		session.execute(text('DROP TABLE file'))

		self.logger.info('Creating the new File and BackupFile tables')
		schema.File.metadata.create_all(self.engine)
		schema.BackupFile.metadata.create_all(self.engine)

		cnt = 0
		last_report_pos = 0
		total: int = session.execute(text('SELECT COUNT(*) FROM old_file')).scalar_one() or 0
		self.logger.info('Total rows in the old "file" table to rebuild: {}'.format(total))

		t = time.time()
		stmt = text('''
			SELECT
				path, mode, content, blob_hash, blob_compress, blob_raw_size, blob_stored_size, uid, gid, ctime_ns, mtime_ns,
				GROUP_CONCAT(backup_id) AS backup_ids,
				GROUP_CONCAT(CASE WHEN atime_ns IS NULL THEN 'NULL' ELSE atime_ns END) AS atime_ns_list 
			FROM 
				old_file 
			GROUP BY 
				path, mode, content, blob_hash, blob_compress, blob_raw_size, blob_stored_size, uid, gid, ctime_ns, mtime_ns
		''')
		for partition in session.execute(stmt).partitions(1000):
			for row in partition:
				# noinspection PyProtectedMember
				old_file = row._asdict()
				backup_ids_str = old_file.pop('backup_ids')
				backup_ids: List[int] = [int(x) for x in backup_ids_str.split(',')]
				atime_list_str = old_file.pop('atime_ns_list')
				atime_list: List[Optional[int]] = [int(x) if x != 'NULL' else None for x in atime_list_str.split(',')]
				assert len(atime_list) == len(backup_ids)

				cnt += len(backup_ids)
				if (p := cnt // 5000) != last_report_pos:
					last_report_pos = p
					self.logger.info('Migrating {} / {} files ({}%)'.format(cnt, total, round(100.0 * cnt / total, 1)))

				file = db_session.create_file_no_add(**old_file)
				db_session.add(file)

				session.flush()
				for i in range(len(backup_ids)):
					db_session.create_backup_file(
						backup_id=backup_ids[i],
						file_id=file.id,
						atime_ns=atime_list[i],
					)

		self.logger.info('Rebuild done, cost {}s'.format(round(time.time() - t, 2)))
		self.logger.info('Cleaning up')
		session.execute(text('DROP TABLE old_file'))

		cnt_f = session.execute(text(f'SELECT COUNT(*) FROM {schema.File.__tablename__}')).scalar_one()
		cnt_bf = session.execute(text(f'SELECT COUNT(*) FROM {schema.BackupFile.__tablename__}')).scalar_one()
		self.logger.info('Done. Constructed {} File and {} BackupFile'.format(cnt_f, cnt_bf))
