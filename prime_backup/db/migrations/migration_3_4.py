import dataclasses
import time
from pathlib import Path

from sqlalchemy import text, inspect, Engine
from sqlalchemy.orm import Session
from typing_extensions import override

from prime_backup.db import schema
from prime_backup.db.migrations import MigrationImplBase


class _V4:
	# FIXME: freeze schema on release
	Base = schema.Base
	Blob = schema.Blob
	Chunk = schema.Chunk
	ChunkGroup = schema.ChunkGroup
	ChunkGroupChunkBinding = schema.ChunkGroupChunkBinding
	BlobChunkGroupBinding = schema.BlobChunkGroupBinding


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

		# 2. rebuild backups
		self.__step_rebuild()
		self.__stats.rebuild_cost = self.__stats.get_elapsed_sec()

		# 3. done
		self.__step_cleanup_and_report()

	def __step_prepare(self):
		# FIXME: TEST

		inspector = inspect(self.engine)
		if 'old_blob_3to4' not in inspector.get_table_names():
			self.session.execute(text('CREATE TABLE old_blob_3to4 AS SELECT * FROM blob'))
		self.session.execute(text('DROP TABLE IF EXISTS blob'))

		self.logger.info('Creating the new chunk tables')
		_V4.Base.metadata.create_all(self.engine, tables=[
			_V4.Base.metadata.tables[declarative.__tablename__]
			for declarative in [
				_V4.Chunk,
				_V4.ChunkGroup,
				_V4.ChunkGroupChunkBinding,
				_V4.BlobChunkGroupBinding,
			]
		])

	def __step_rebuild(self):
		# rebuild blobs
		self.session.execute(text('''
			INSERT INTO blob (storage_method, hash, compress, raw_size, stored_size)
			SELECT 1, hash, compress, raw_size, stored_size
			FROM old_blob_3to4
		'''))

		# TODO: rebuild mtime?

	def __step_cleanup_and_report(self):
		self.logger.info('Migration 2to3 done, cost {}s (prepare {}s, rebuild {}s)'.format(
			round(self.__stats.get_elapsed_sec(), 2),
			round(self.__stats.prepare_cost, 2),
			round(self.__stats.rebuild_cost, 2),
		))
		self.logger.info('Cleaning up')
		self.session.execute(text('DROP TABLE old_blob_3to4'))

		cnt_blob = self.session.execute(text(f'SELECT COUNT(*) FROM blob')).scalar_one()
		self.logger.info('Done. Reconstructed {} blobs in total'.format(cnt_blob))
