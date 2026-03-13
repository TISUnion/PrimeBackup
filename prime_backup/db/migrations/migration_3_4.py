import dataclasses
import time
from pathlib import Path
from typing import Dict

from sqlalchemy import text, inspect, Engine
from sqlalchemy.orm import Session
from typing_extensions import override

from prime_backup.db import schema
from prime_backup.db.migrations import MigrationImplBase
from prime_backup.db.values import BlobStorageMethod


class _V4:
	# FIXME: freeze schema on release
	Base = schema.Base
	Blob = schema.Blob
	Chunk = schema.Chunk
	ChunkGroup = schema.ChunkGroup
	ChunkGroupChunkBinding = schema.ChunkGroupChunkBinding
	BlobChunkGroupBinding = schema.BlobChunkGroupBinding
	File = schema.File
	Backup = schema.Backup


@dataclasses.dataclass
class _Stats:
	start_ts: float = 0
	prepare_cost: float = 0
	rebuild_cost: float = 0

	def get_elapsed_sec(self) -> float:
		return time.time() - self.start_ts


class MigrationImpl3To4(MigrationImplBase):
	def __init__(self, engine: Engine, temp_dir: Path, session: Session):
		super().__init__(engine, temp_dir, session)
		self.__stats = _Stats()

	@override
	def _migrate(self):
		self.__stats.start_ts = time.time()

		# 1. prepare database for migration. relocate old tables and (re)create new tables
		self.__step_prepare()
		self.__stats.prepare_cost = self.__stats.get_elapsed_sec()

		# 2. rebuild DB
		self.__step_rebuild()
		self.__stats.rebuild_cost = self.__stats.get_elapsed_sec()

		# 3. done
		self.__step_cleanup_and_report()

	def __step_prepare(self):
		# FIXME: TEST

		inspector = inspect(self.engine)
		if 'old_blob_3to4' not in inspector.get_table_names():
			self.session.execute(text('CREATE TABLE old_blob_3to4 AS SELECT * FROM blob'))
		if 'old_file_3to4' not in inspector.get_table_names():
			self.session.execute(text('CREATE TABLE old_file_3to4 AS SELECT * FROM file'))
		if 'old_backup_3to4' not in inspector.get_table_names():
			self.session.execute(text('CREATE TABLE old_backup_3to4 AS SELECT * FROM backup'))
		self.session.execute(text('DROP TABLE IF EXISTS blob'))
		self.session.execute(text('DROP TABLE IF EXISTS file'))
		self.session.execute(text('DROP TABLE IF EXISTS backup'))

		# Drop redundant index on fileset.id (primary key already has index)
		self.logger.info('Dropping redundant index on fileset.id')
		self.session.execute(text('DROP INDEX IF EXISTS ix_fileset_id'))

		self.logger.info('Creating the new chunk tables')
		_V4.Base.metadata.create_all(self.engine, tables=[
			_V4.Base.metadata.tables[declarative.__tablename__]
			for declarative in [
				_V4.Blob,
				_V4.Chunk,
				_V4.ChunkGroup,
				_V4.ChunkGroupChunkBinding,
				_V4.BlobChunkGroupBinding,
				_V4.File,
				_V4.Backup,
			]
		])

	def __step_rebuild(self):
		def get_mapped_insert_sql(src_table: str, dst_table: str, mapping: Dict[str, str]):
			select_fields = ', '.join(mapping.keys())
			insert_fields = ', '.join(mapping.values())
			sql = f'''
				INSERT INTO {dst_table} ({select_fields})
				SELECT {insert_fields}
				FROM {src_table}
			'''
			self.logger.debug('mapped insert {} -> {}: {!r}'.format(src_table, dst_table, sql))
			return text(sql)

		# rebuild blobs
		self.logger.info('(blob table) Migrating data from old table')
		self.session.execute(get_mapped_insert_sql('old_blob_3to4', 'blob', {
			'storage_method': str(BlobStorageMethod.direct.value),
			**{name: name for name in ['hash', 'compress', 'raw_size', 'stored_size']},
		}))

		# rebuild files
		self.logger.info('(file table) Migrating data from old table')
		self.session.execute(get_mapped_insert_sql('old_file_3to4', 'file', {
			**{name: name for name in ['fileset_id', 'path', 'role', 'mode', 'content']},
			'blob_id': 'NULL',
			'blob_storage_method': 'NULL',
			**{name: name for name in [
				'blob_hash', 'blob_compress', 'blob_raw_size', 'blob_stored_size',
				'uid', 'gid', 'mtime',
			]},
			'mtime_ns_part': '0',
		}))
		self.logger.info('(file table) Filling blob_id column')
		self.session.execute(text('''
			UPDATE file SET blob_id = (
				SELECT blob.id
					FROM blob
					WHERE blob.hash = file.blob_hash
			)
			WHERE file.blob_hash IS NOT NULL
		'''))
		self.logger.info('(file table) Filling blob_storage_method column')
		self.session.execute(text('''
			UPDATE file SET blob_storage_method = :blob_storage_method
			WHERE file.blob_hash IS NOT NULL
		''').bindparams(blob_storage_method=str(BlobStorageMethod.direct.value))),
		self.logger.info('(file table) Updating mtime columns')  # old mtime was stored in us
		self.session.execute(text('''UPDATE file SET mtime_ns_part = mtime % 1000000 * 1000'''))
		self.session.execute(text('''UPDATE file SET mtime = mtime / 1000000'''))

		# rebuild backups
		self.logger.info('(backup table) Migrating data from old table')
		self.session.execute(get_mapped_insert_sql('old_backup_3to4', 'backup', {
			**{name: name for name in ['id', 'timestamp']},
			'timestamp_ns_part': '0',
			**{name: name for name in [
				'creator', 'comment', 'targets', 'tags',
				'fileset_id_base', 'fileset_id_delta', 'file_count', 'file_raw_size_sum', 'file_stored_size_sum',
			]},
		}))
		self.logger.info('(backup table) Updating timestamp columns')  # old timestamp was stored in us
		self.session.execute(text('''UPDATE backup SET timestamp_ns_part = timestamp % 1000000 * 1000'''))
		self.session.execute(text('''UPDATE backup SET timestamp = timestamp / 1000000'''))

	def __step_cleanup_and_report(self):
		self.logger.info('Migration 3to4 done, cost {}s (prepare {}s, rebuild {}s)'.format(
			round(self.__stats.get_elapsed_sec(), 2),
			round(self.__stats.prepare_cost, 2),
			round(self.__stats.rebuild_cost, 2),
		))
		self.logger.info('Cleaning up')
		self.session.execute(text('DROP TABLE old_blob_3to4'))
		self.session.execute(text('DROP TABLE old_file_3to4'))
		self.session.execute(text('DROP TABLE old_backup_3to4'))

		cnt_blob = self.session.execute(text(f'SELECT COUNT(*) FROM blob')).scalar_one()
		cnt_file = self.session.execute(text(f'SELECT COUNT(*) FROM file')).scalar_one()
		cnt_backup = self.session.execute(text(f'SELECT COUNT(*) FROM backup')).scalar_one()
		self.logger.info('Done. Reconstructed {} blobs, {} files and {} backups in total'.format(cnt_blob, cnt_file, cnt_backup))
