import collections
import dataclasses
import json
import time
from pathlib import Path
from typing import List, Dict

from sqlalchemy import text, inspect, Engine, RowMapping
from sqlalchemy.orm import Session
from typing_extensions import override

from prime_backup.db import schema
from prime_backup.db.migrations import MigrationImplBase
from prime_backup.utils import collection_utils
from prime_backup.utils.lru_dict import LruDict


class _V3:
	Base = schema.Base
	File = schema.File
	Fileset = schema.Fileset
	Backup = schema.Backup


@dataclasses.dataclass
class _Stats:
	start_ts: float = 0
	prepare_cost: float = 0
	rebuild_cost: float = 0

	old_backup_count: int = 0
	old_file_count: int = 0
	processed_backup_count: int = 0

	def get_elapsed_sec(self) -> float:
		return time.time() - self.start_ts


class MigrationImpl2To3(MigrationImplBase):
	def __init__(self, engine: Engine, temp_dir: Path, session: Session):
		super().__init__(engine, temp_dir, session)
		self.__stats = _Stats()
		self.__fileset_files_cache = LruDict(max_size=8)

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
		inspector = inspect(self.engine)
		if 'old_file_2to3' not in inspector.get_table_names():
			self.logger.info('Relocating the old "file" table to "old_file_2to3')
			self.session.execute(text('CREATE TABLE old_file_2to3 AS SELECT * FROM file'))

		if 'old_backup_2to3' not in inspector.get_table_names():
			self.logger.info('Relocating the old "backup" table to "old_backup_2to3"')
			self.session.execute(text('CREATE TABLE old_backup_2to3 AS SELECT * FROM backup'))

		self.session.execute(text('DROP TABLE IF EXISTS file'))
		self.session.execute(text('DROP TABLE IF EXISTS backup'))
		self.session.execute(text('DROP TABLE IF EXISTS fileset'))

		# FIXME: This might not work if schema is updated in the future
		# temp workaround to make it running first
		self.logger.info('Creating the new File and Fileset tables')
		_V3.Base.metadata.create_all(self.engine, tables=[_V3.Base.metadata.tables[table_name] for table_name in ['file', 'fileset', 'backup']])

	def __step_rebuild(self):
		old_backup_ids: List[int] = sorted(set(self.session.execute(text('SELECT id FROM old_backup_2to3')).scalars()))
		self.__stats.old_file_count = self.session.execute(text('SELECT COUNT(*) FROM old_file_2to3')).scalar_one() or 0
		self.__stats.old_backup_count = len(old_backup_ids)
		self.logger.info('Total rows to rebuild: {} backups, {} files'.format(self.__stats.old_backup_count, self.__stats.old_file_count))

		start_ts = time.time()

		for backup_id_batch in collection_utils.slicing_iterate(old_backup_ids, 10):
			sql_in_arg = repr(tuple(map(int, backup_id_batch)))
			old_backup_rows = list(self.session.execute(text(f'SELECT * FROM old_backup_2to3 WHERE id IN {sql_in_arg}')))
			old_file_rows = list(self.session.execute(text(f'SELECT * FROM old_file_2to3 WHERE backup_id IN {sql_in_arg}')))
			self.logger.debug('Selected {} old backups with {} old file'.format(len(backup_id_batch), len(old_file_rows)))

			old_backup_by_backup_id: Dict[int, RowMapping] = {}
			for old_backup_row in old_backup_rows:
				# noinspection PyProtectedMember
				old_backup_row_mapping = old_backup_row._mapping
				old_backup_by_backup_id[old_backup_row_mapping['id']] = old_backup_row_mapping

			old_file_rows_by_backup_id: Dict[int, List[RowMapping]] = collections.defaultdict(list)
			for old_file_row in old_file_rows:
				# noinspection PyProtectedMember
				old_file_row_mapping = old_file_row._mapping
				old_file_rows_by_backup_id[old_file_row_mapping['backup_id']].append(old_file_row_mapping)

			for backup_id in backup_id_batch:
				old_backup = old_backup_by_backup_id[backup_id]
				old_file_rows = old_file_rows_by_backup_id[backup_id]
				self.logger.debug('Rebuilding backup {} with {} files'.format(backup_id, len(old_file_rows)))

				new_backup = self.__rebuild_backup(backup_id, old_backup, old_file_rows)

				percent = 100.0 * self.__stats.processed_backup_count / self.__stats.old_backup_count
				elapsed_sec = time.time() - start_ts
				self.logger.info('Rebuilt backup {} with {} files ({} / {}, {:.2f}%, elapsed {}s, rebuild eta {}s)'.format(
					new_backup.id, new_backup.file_count, self.__stats.processed_backup_count, self.__stats.old_backup_count, percent,
					round(elapsed_sec), round(elapsed_sec / self.__stats.processed_backup_count * max(0, self.__stats.old_backup_count - self.__stats.processed_backup_count)),
				))

	def __rebuild_backup(self, backup_id: int, old_backup: RowMapping, old_file_rows: List[RowMapping]) -> _V3.Backup:
		files: List[_V3.File] = []
		for old_file_row in old_file_rows:
			fields = {
				key: old_file_row[key]
				for key in [
					'path',
					'mode',
					'content',
					'blob_hash',
					'blob_compress',
					'blob_raw_size',
					'blob_stored_size',
					'uid',
					'gid',
					'mtime_ns',
				]
			}
			fields['mtime'] = fields.pop('mtime_ns')
			files.append(_V3.File(**fields))

		# FIXME: This might not work if impl is updated in the future
		from prime_backup.action.helpers.fileset_allocator import FilesetAllocator, FilesetAllocateArgs
		from prime_backup.db.session import DbSession
		allocator = FilesetAllocator(
			DbSession(self.session), files,
			migration2to3_mode=True,
			fileset_files_cache=self.__fileset_files_cache,
		)
		fs_result = allocator.allocate(FilesetAllocateArgs())
		self.logger.debug('Allocated fileset for the backup files, backup_id {}'.format(backup_id))

		fs_base, fs_delta = fs_result.fileset_base, fs_result.fileset_delta
		new_backup = _V3.Backup(
			id=old_backup['id'],
			timestamp=old_backup['timestamp'],
			creator=old_backup['creator'],
			comment=old_backup['comment'],
			targets=json.loads(old_backup['targets']),
			tags=json.loads(old_backup['tags']),
			fileset_id_base=fs_base.id,
			fileset_id_delta=fs_delta.id,
			file_count=fs_base.file_count + fs_delta.file_count,
			file_raw_size_sum=fs_base.file_raw_size_sum + fs_delta.file_raw_size_sum,
			file_stored_size_sum=fs_base.file_stored_size_sum + fs_delta.file_stored_size_sum,
		)
		self.session.add(new_backup)
		self.session.flush()

		self.__stats.processed_backup_count += 1
		files.clear()
		return new_backup

	def __step_cleanup_and_report(self):
		self.logger.info('Migration 2to3 done, cost {}s (prepare {}s, rebuild {}s)'.format(
			round(self.__stats.get_elapsed_sec(), 2),
			round(self.__stats.prepare_cost, 2),
			round(self.__stats.rebuild_cost, 2),
		))
		self.logger.info('Cleaning up')
		self.session.execute(text('DROP TABLE old_file_2to3'))
		self.session.execute(text('DROP TABLE old_backup_2to3'))

		cnt_f = self.session.execute(text(f'SELECT COUNT(*) FROM {_V3.File.__tablename__}')).scalar_one()
		cnt_fs = self.session.execute(text(f'SELECT COUNT(*) FROM {_V3.Fileset.__tablename__}')).scalar_one()
		cnt_b = self.session.execute(text(f'SELECT COUNT(*) FROM {_V3.Backup.__tablename__}')).scalar_one()
		self.logger.info('Done. Constructed {} files (decreased from {}, {:.1f}%), {} filesets, {} backups'.format(
			cnt_f, self.__stats.old_file_count, (cnt_f - self.__stats.old_file_count) / self.__stats.old_file_count * 100,
			cnt_fs, cnt_b,
		))
