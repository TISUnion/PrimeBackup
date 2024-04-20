import shutil
import time
from pathlib import Path
from typing import List, Tuple, Set

from prime_backup.action import Action
from prime_backup.compressors import CompressMethod, Compressor
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.types.size_diff import SizeDiff
from prime_backup.utils import blob_utils

_OLD_BLOB_SUFFIX = '_old'


class MigrateCompressMethodAction(Action[SizeDiff]):
	def __init__(self, new_compress_method: CompressMethod):
		super().__init__()
		self.new_compress_method = new_compress_method
		self.__migrated_blob_hashes: List[str] = []
		self.__affected_backup_ids: Set[int] = set()

	@classmethod
	def __get_blob_paths(cls, h: str) -> Tuple[Path, Path]:
		blob_path = blob_utils.get_blob_path(h)
		old_trash_path = blob_path.parent / (blob_path.name + _OLD_BLOB_SUFFIX)
		return blob_path, old_trash_path

	def __migrate_blob(self, blob: schema.Blob) -> bool:
		new_compress_method = self.config.backup.get_compress_method_from_size(blob.raw_size, compress_method_override=self.new_compress_method)
		decompressor = Compressor.create(blob.compress)
		compressor = Compressor.create(new_compress_method)
		if decompressor.get_method() == compressor.get_method():
			return False

		blob_path, old_trash_path = self.__get_blob_paths(blob.hash)
		blob_path.replace(old_trash_path)
		with decompressor.open_decompressed(old_trash_path) as f_src:
			with compressor.open_compressed_bypassed(blob_path) as (writer, f_dst):
				shutil.copyfileobj(f_src, f_dst)

		blob.compress = new_compress_method.name
		blob.stored_size = writer.get_write_len()
		return True

	def __migrate_blobs_and_sync_files(self, session: DbSession, blobs: List[schema.Blob]):
		blob_mapping = {}
		for blob in blobs:
			try:
				changed = self.__migrate_blob(blob)
			except Exception as e:
				self.logger.error('Migrate blob {} failed: {}'.format(blob, e))
				raise

			if changed:
				blob_mapping[blob.hash] = blob
				self.__migrated_blob_hashes.append(blob.hash)

		for file in session.get_file_by_blob_hashes(list(blob_mapping.keys())):
			blob = blob_mapping[file.blob_hash]
			file.blob_compress = blob.compress
			file.blob_stored_size = blob.stored_size
			self.__affected_backup_ids.add(file.backup_id)

	def __update_backups(self, session: DbSession):
		backup_ids = list(sorted(self.__affected_backup_ids))
		backups = session.get_backups(backup_ids)
		for backup_id in backup_ids:
			backup = backups[backup_id]
			backup.file_stored_size_sum = session.calc_file_stored_size_sum(backup.id)

	def __erase_old_blobs(self):
		for h in self.__migrated_blob_hashes:
			_, old_trash_path = self.__get_blob_paths(h)
			old_trash_path.unlink()

	def __rollback(self):
		for h in self.__migrated_blob_hashes:
			blob_path, old_trash_path = self.__get_blob_paths(h)
			if old_trash_path.is_file():
				old_trash_path.replace(blob_path)

	def run(self) -> SizeDiff:
		# Notes: requires 2x disk usage of the blob store, stores all blob hashes in memory
		self.__migrated_blob_hashes.clear()
		self.logger.info('Migrating compress method to {} (compress threshold = {})'.format(self.new_compress_method.name, self.config.backup.compress_threshold))

		try:
			# Blob operation steps:
			# 1. move xxx -> xxx_old
			# 2. copy xxx_old --[migrate]-> xxx
			# 3. delete xxx_old
			with DbAccess.open_session() as session:
				# 0. fetch information before the migration
				t = time.time()
				before_size = session.get_blob_stored_size_sum()
				total_blob_count = session.get_blob_count()

				# 1. migrate blob objects
				cnt = 0
				for blobs in session.iterate_blob_batch(batch_size=1000):
					cnt += len(blobs)
					self.logger.info('Processing blobs {} / {}'.format(cnt, total_blob_count))
					self.__migrate_blobs_and_sync_files(session, blobs)
					session.flush_and_expunge_all()

				if len(self.__migrated_blob_hashes) == 0:
					self.logger.info('No blob needs a compress method change, nothing to migrate')
				else:
					self.logger.info('Migrated {} blobs and related files'.format(len(self.__migrated_blob_hashes)))

					# 3. migrate backup data
					self.logger.info('Syncing {} affected backups'.format(len(self.__affected_backup_ids)))
					self.__update_backups(session)
					session.flush_and_expunge_all()

				# 4. output
				after_size = session.get_blob_stored_size_sum()

		except Exception:
			self.logger.warning('Error occurs during compress method migration, applying rollback')
			self.__rollback()
			raise

		else:
			# 5. migration done, do some cleanup
			self.logger.info('Cleaning up old blobs')
			self.__erase_old_blobs()

			self.config.backup.compress_method = self.new_compress_method
			self.logger.info('Compress method migration done, cost {}s'.format(round(time.time() - t, 2)))
			return SizeDiff(before_size, after_size)

		finally:
			self.__migrated_blob_hashes.clear()
