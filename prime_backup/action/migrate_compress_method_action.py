import time
from concurrent.futures import Future
from pathlib import Path
from typing import List, Tuple, Set, Dict

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.compressors import CompressMethod, Compressor
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.db.values import BlobStorageMethod
from prime_backup.types.size_diff import SizeDiff
from prime_backup.utils import blob_utils, chunk_utils, file_utils
from prime_backup.utils.thread_pool import FailFastBlockingThreadPool

_OLD_BLOB_SUFFIX = '_old'


class MigrateCompressMethodAction(Action[SizeDiff]):
	def __init__(self, new_compress_method: CompressMethod):
		super().__init__()
		self.new_compress_method = new_compress_method

		# records changed files
		self.__migrated_blob_hashes: List[str] = []
		self.__migrated_chunk_hashes: List[str] = []

		# records affects stuffs for next migration phase
		self.__affected_chunk_groups_ids: Set[int] = set()
		self.__affected_fileset_ids: Set[int] = set()

	def __update_files_for_blob_change(self, session: DbSession, changed_blobs_by_hash: Dict[str, schema.Blob]):
		for file in session.get_file_by_blob_hashes(list(changed_blobs_by_hash.keys())):
			if file.blob_hash is None:
				raise AssertionError('File {!r} has no blob_hash'.format(file))
			blob = changed_blobs_by_hash[file.blob_hash]
			file.blob_compress = blob.compress
			file.blob_stored_size = blob.stored_size
			self.__affected_fileset_ids.add(file.fileset_id)

	@classmethod
	def __get_blob_paths(cls, h: str) -> Tuple[Path, Path]:
		blob_path = blob_utils.get_blob_path(h)
		old_trash_path = blob_path.parent / (blob_path.name + _OLD_BLOB_SUFFIX)
		return blob_path, old_trash_path

	@classmethod
	def __get_chunk_paths(cls, h: str) -> Tuple[Path, Path]:
		chunk_path = chunk_utils.get_chunk_path(h)
		old_trash_path = chunk_path.parent / (chunk_path.name + _OLD_BLOB_SUFFIX)
		return chunk_path, old_trash_path

	def __migrate_single_direct_blob(self, blob: schema.Blob) -> bool:
		new_compress_method = self.config.backup.get_compress_method_from_size(blob.raw_size, compress_method_override=self.new_compress_method)
		decompressor = Compressor.create(blob.compress)
		compressor = Compressor.create(new_compress_method)
		if decompressor.get_method() == compressor.get_method():
			return False

		blob_path, old_trash_path = self.__get_blob_paths(blob.hash)
		blob_path.replace(old_trash_path)
		with decompressor.open_decompressed(old_trash_path) as f_src:
			with compressor.open_compressed_bypassed(blob_path) as (writer, f_dst):
				file_utils.copy_file_obj_fast(f_src, f_dst, estimate_read_size=blob.stored_size)

		blob.compress = new_compress_method.name
		blob.stored_size = writer.get_write_len()
		return True

	def __migrate_direct_blobs_and_sync_files(self, session: DbSession, blobs: List[schema.Blob]):
		changed_blobs_by_hash: Dict[str, schema.Blob] = {}

		with FailFastBlockingThreadPool('migration') as pool:
			future_pairs: List[Tuple[schema.Blob, 'Future[bool]']] = []
			for blob in blobs:
				future = pool.submit(self.__migrate_single_direct_blob, blob)
				future_pairs.append((blob, future))
			for blob, future in future_pairs:
				try:
					changed = future.result()
				except Exception as e:
					self.logger.error('Migrate blob {} failed: {}'.format(blob, e))
					raise
				if changed:
					changed_blobs_by_hash[blob.hash] = blob
					self.__migrated_blob_hashes.append(blob.hash)

		self.__update_files_for_blob_change(session, changed_blobs_by_hash)

	def __migrate_single_chunk(self, chunk: schema.Chunk) -> bool:
		new_compress_method = self.config.backup.get_compress_method_from_size(chunk.raw_size, compress_method_override=self.new_compress_method)
		decompressor = Compressor.create(chunk.compress)
		compressor = Compressor.create(new_compress_method)
		if decompressor.get_method() == compressor.get_method():
			return False

		chunk_path, old_trash_path = self.__get_chunk_paths(chunk.hash)
		chunk_path.replace(old_trash_path)
		with decompressor.open_decompressed(old_trash_path) as f_src:
			with compressor.open_compressed_bypassed(chunk_path) as (writer, f_dst):
				file_utils.copy_file_obj_fast(f_src, f_dst, estimate_read_size=chunk.stored_size)

		chunk.compress = new_compress_method.name
		chunk.stored_size = writer.get_write_len()
		return True

	def __migrate_chunks(self, session: DbSession, chunks: List[schema.Chunk]):
		changed_chunk_ids: Set[int] = set()

		with FailFastBlockingThreadPool('migration') as pool:
			future_pairs: List[Tuple[schema.Chunk, 'Future[bool]']] = []
			for chunk in chunks:
				future = pool.submit(self.__migrate_single_chunk, chunk)
				future_pairs.append((chunk, future))
			for chunk, future in future_pairs:
				try:
					changed = future.result()
				except Exception as e:
					self.logger.error('Migrate chunk {} failed: {}'.format(chunk, e))
					raise
				if changed:
					changed_chunk_ids.add(chunk.id)
					self.__migrated_chunk_hashes.append(chunk.hash)

		self.__affected_chunk_groups_ids.update(session.get_chunk_group_ids_by_chunk_ids(list(changed_chunk_ids)))

	def __update_chunk_group_and_blobs_for_chunk_changes(self, session: DbSession):
		if len(self.__affected_chunk_groups_ids) == 0:
			return

		chunk_group_id_list = list(self.__affected_chunk_groups_ids)
		chunk_groups = session.get_chunk_groups_by_ids(chunk_group_id_list)
		for chunk_group_id, chunk_group in chunk_groups.items():
			if chunk_group is None:
				raise AssertionError('Chunk group with id {!r} does not exists'.format(chunk_group_id))
			chunk_group.chunk_stored_size_sum = session.calc_chunk_group_stored_size_sum(chunk_group.id)

		affected_blobs = session.get_blobs_by_chunk_group_ids(chunk_group_id_list)
		for blob in affected_blobs:
			if blob.storage_method != BlobStorageMethod.chunked.value:
				raise AssertionError('Blob {!r} is not a chunked blob'.format(blob))
			blob.stored_size = session.calc_chunked_blob_stored_size_sum(blob.id)
		self.__update_files_for_blob_change(session, {blob.hash: blob for blob in affected_blobs})

		self.logger.info('Syncing {} affected chunk groups and {} affected blobs for chunk changes'.format(len(chunk_groups), len(affected_blobs)))
		session.flush_and_expunge_all()

	def __update_fileset_and_backups(self, session: DbSession):
		if len(self.__affected_fileset_ids) == 0:
			return

		fileset_ids = set(self.__affected_fileset_ids)
		backup_ids = session.get_backup_ids_by_fileset_ids(list(fileset_ids))
		self.logger.info('Syncing {} affected filesets and {} associated backups'.format(len(fileset_ids), len(backup_ids)))

		filesets = session.get_filesets(list(fileset_ids))
		for fileset in filesets.values():
			fileset.file_stored_size_sum = session.calc_file_stored_size_sum(fileset.id)

		all_backup_fileset_ids: Set[int] = set()
		backups = list(session.get_backups(backup_ids).values())
		for backup in backups:
			all_backup_fileset_ids.add(backup.fileset_id_base)
			all_backup_fileset_ids.add(backup.fileset_id_delta)
		more_filesets = session.get_filesets(list(all_backup_fileset_ids.difference(fileset_ids)))
		filesets.update(more_filesets)  # batch query, faster

		for backup in backups:
			fs_base = filesets[backup.fileset_id_base]
			fs_delta = filesets[backup.fileset_id_delta]
			backup.file_stored_size_sum = fs_base.file_stored_size_sum + fs_delta.file_stored_size_sum
		session.flush_and_expunge_all()

	def __erase_old_blob_and_chunk_files(self):
		for h in self.__migrated_blob_hashes:
			_, old_trash_path = self.__get_blob_paths(h)
			old_trash_path.unlink()
		for h in self.__migrated_chunk_hashes:
			_, old_trash_path = self.__get_chunk_paths(h)
			old_trash_path.unlink()

	def __rollback(self):
		for h in self.__migrated_blob_hashes:
			blob_path, old_trash_path = self.__get_blob_paths(h)
			if old_trash_path.is_file():
				old_trash_path.replace(blob_path)
		for h in self.__migrated_chunk_hashes:
			chunk_path, old_trash_path = self.__get_chunk_paths(h)
			if old_trash_path.is_file():
				old_trash_path.replace(chunk_path)

	@override
	def run(self) -> SizeDiff:
		# Notes: requires 2x disk usage of the blob store, stores all blob hashes in memory
		self.logger.info('Migrating compress method to {} (compress threshold = {})'.format(self.new_compress_method.name, self.config.backup.compress_threshold))

		try:
			# Blob operation steps:
			# 1. move xxx -> xxx_old
			# 2. copy xxx_old --[migrate]-> xxx
			# 3. delete xxx_old
			with DbAccess.open_session() as session:
				# 0. fetch information before the migration
				t = time.time()
				before_size = session.get_blob_store_fs_file_size_sum()

				# 1. migrate direct blob objects
				cnt = 0
				total_blob_count = session.get_blob_count()
				for blobs in session.iterate_blob_batch(batch_size=1000):
					cnt += len(blobs)
					self.logger.info('Processing blobs {} / {}'.format(cnt, total_blob_count))
					self.__migrate_direct_blobs_and_sync_files(session, [
						blob for blob in blobs
						if blob.storage_method == BlobStorageMethod.direct.value
					])
					session.flush_and_expunge_all()
				if len(self.__migrated_blob_hashes) == 0:
					self.logger.info('No blob needs a compress method change, nothing to migrate')
				else:
					self.logger.info('Migrated {} direct blobs and related files'.format(len(self.__migrated_blob_hashes)))

				# 2. migrate chunks objects, and sync chunk groups and chunked blobs
				cnt = 0
				total_chunk_count = session.get_chunk_count()
				for chunks in session.iterate_chunk_batch(batch_size=1000):
					cnt += len(chunks)
					self.logger.info('Processing chunks {} / {}'.format(cnt, total_chunk_count))
					self.__migrate_chunks(session, chunks)
					session.flush_and_expunge_all()
				if len(self.__migrated_chunk_hashes) == 0:
					self.logger.info('No chunk needs a compress method change, nothing to migrate')
				else:
					self.logger.info('Migrated {} chunks and related blob and files'.format(len(self.__migrated_chunk_hashes)))

				# 3. migrate affected fileset and backup data
				self.__update_chunk_group_and_blobs_for_chunk_changes(session)
				self.__update_fileset_and_backups(session)

				# 4. output
				after_size = session.get_blob_store_fs_file_size_sum()

		except Exception:
			self.logger.warning('Error occurs during compress method migration, applying rollback')
			self.__rollback()
			raise

		else:
			# 5. migration done, do some cleanup
			self.logger.info('Cleaning up old blob and chunk files')
			self.__erase_old_blob_and_chunk_files()

			self.config.backup.compress_method = self.new_compress_method
			self.logger.info('Compress method migration done, cost {}s'.format(round(time.time() - t, 2)))
			return SizeDiff(before_size, after_size)

		finally:
			self.__migrated_blob_hashes.clear()
