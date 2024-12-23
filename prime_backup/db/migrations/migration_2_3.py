import collections
import json
import time
from typing import List, Deque

from sqlalchemy import text, inspect

from prime_backup.db import schema
from prime_backup.db.migrations import MigrationImplBase
from prime_backup.utils.lru_dict import LruDict


class _V3:
	Base = schema.Base
	File = schema.File
	Fileset = schema.Fileset
	Backup = schema.Backup


class MigrationImpl2To3(MigrationImplBase):
	def migrate(self):
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

		old_file_count: int = self.session.execute(text('SELECT COUNT(*) FROM old_file_2to3')).scalar_one() or 0
		old_backup_ids: List[int] = sorted(set(self.session.execute(text('SELECT id FROM old_backup_2to3')).scalars()))
		old_backup_count = len(old_backup_ids)
		self.logger.info('Total rows to rebuild: {} backups, {} files'.format(old_backup_count, old_file_count))
		remaining_backup_ids: Deque[int] = collections.deque(old_backup_ids)

		from prime_backup.db.session import DbSession
		db_session = DbSession(self.session)

		start_ts = time.time()
		files: List[_V3.File] = []
		processed_backup_count = 0

		# FIXME: This might not work if impl is updated in the future
		from prime_backup.action.helpers.fileset_allocator import FilesetAllocator, FilesetAllocateArgs
		allocate_args = FilesetAllocateArgs()
		fileset_files_cache = LruDict(max_size=allocate_args.max_base_reuse_count)

		def finalize_files(backup_id: int):
			self.logger.debug('Processing backup {} with {} files'.format(backup_id, len(files)))
			old_backup_row = self.session.execute(
				text('SELECT * FROM old_backup_2to3 where id = :backup_id').bindparams(backup_id=backup_id)
			).one()

			# noinspection PyProtectedMember
			old_backup = old_backup_row._mapping

			allocator = FilesetAllocator(db_session, files)
			allocator.enable_fileset_files_cache(fileset_files_cache)
			fs_result = allocator.allocate(allocate_args)

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

			nonlocal processed_backup_count
			processed_backup_count += 1
			percent = 100.0 * processed_backup_count / old_backup_count
			elapsed_sec = time.time() - start_ts
			self.logger.info('Processed backup {} with {} files ({} / {}, {:.2f}%, elapsed {}s, eta {}s)'.format(
				new_backup.id, new_backup.file_count, processed_backup_count, old_backup_count, percent,
				round(elapsed_sec), round(elapsed_sec / processed_backup_count * max(0, old_backup_count - processed_backup_count)),
			))

			files.clear()

		for file_row in self.session.execute(text('SELECT * FROM old_file_2to3 ORDER BY backup_id')).yield_per(1000):
			# noinspection PyProtectedMember
			old_file = file_row._mapping

			while len(remaining_backup_ids) > 0 and remaining_backup_ids[0] != old_file['backup_id']:
				finalize_files(remaining_backup_ids.popleft())
			if len(remaining_backup_ids) == 0:
				raise AssertionError('Unexpected drained remaining_backup_ids, {} {}'.format(old_file['backup_id'], old_backup_ids))

			files.append(_V3.File(**{
				key: old_file[key]
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
			}))

		while len(remaining_backup_ids) > 0:
			finalize_files(remaining_backup_ids.popleft())

		self.logger.info('Rebuild done, cost {}s'.format(round(time.time() - start_ts, 2)))
		self.logger.info('Cleaning up')
		self.session.execute(text('DROP TABLE old_file_2to3'))
		self.session.execute(text('DROP TABLE old_backup_2to3'))

		cnt_f = self.session.execute(text(f'SELECT COUNT(*) FROM {_V3.File.__tablename__}')).scalar_one()
		cnt_fs = self.session.execute(text(f'SELECT COUNT(*) FROM {_V3.Fileset.__tablename__}')).scalar_one()
		cnt_b = self.session.execute(text(f'SELECT COUNT(*) FROM {_V3.Backup.__tablename__}')).scalar_one()
		self.logger.info('Done. Constructed {} files (decreased from {}, {:.1f}%), {} filesets, {} backups'.format(
			cnt_f, old_file_count, (cnt_f - old_file_count) / old_file_count * 100,
			cnt_fs, cnt_b,
		))
