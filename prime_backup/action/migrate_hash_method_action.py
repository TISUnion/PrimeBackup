import shutil
import time
from typing import List, Dict, Set

from prime_backup.action import Action
from prime_backup.compressors import Compressor
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.exceptions import PrimeBackupError
from prime_backup.types.hash_method import HashMethod
from prime_backup.utils import blob_utils, hash_utils, collection_utils


class HashCollisionError(PrimeBackupError):
	"""
	Same hash value, between 2 hash methods
	"""
	pass


class MigrateHashMethodAction(Action[None]):
	def __init__(self, new_hash_method: HashMethod):
		super().__init__()
		self.new_hash_method = new_hash_method

	def __migrate_blobs(self, session: DbSession, blob_hashes: List[str], old_hashes: Set[str], processed_hash_mapping: Dict[str, str]):
		hash_mapping: Dict[str, str] = {}
		blobs = list(session.get_blobs(blob_hashes).values())

		# calc blob hashes
		for blob in blobs:
			blob_path = blob_utils.get_blob_path(blob.hash)
			with Compressor.create(blob.compress).open_decompressed(blob_path) as f:
				sah = hash_utils.calc_reader_size_and_hash(f, hash_method=self.new_hash_method)
			hash_mapping[blob.hash] = sah.hash
			if sah.hash in old_hashes:
				raise HashCollisionError(sah.hash)

		# update the objects
		for blob in blobs:
			old_hash, new_hash = blob.hash, hash_mapping[blob.hash]
			old_path = blob_utils.get_blob_path(old_hash)
			new_path = blob_utils.get_blob_path(new_hash)
			try:
				shutil.move(old_path, new_path)
			except Exception as e:
				self.logger.error('Move blob ({} -> {}) from {!r} to {!r} failed: {}'.format(old_hash, new_hash, old_path, new_path, e))
				raise

			processed_hash_mapping[old_hash] = new_hash
			blob.hash = new_hash

		for file in session.get_file_by_blob_hashes(list(hash_mapping.keys())):
			file.blob_hash = hash_mapping[file.blob_hash]

	def run(self):
		processed_hash_mapping: Dict[str, str] = {}  # old -> new
		try:
			t = time.time()
			with DbAccess.open_session() as session:
				meta = session.get_db_meta()
				if meta.hash_method == self.new_hash_method.name:
					self.logger.info('Hash method of the database is already {}, no need to migrate'.format(self.new_hash_method.name))
					return

				self.logger.info('Migrating hash method from {} to {}'.format(meta.hash_method, self.new_hash_method.name))

				total_blob_count = session.get_blob_count()
				all_hashes = session.get_all_blob_hashes()
				all_hash_set = set(all_hashes)
				cnt = 0
				for blob_hashes in collection_utils.slicing_iterate(all_hashes, 1000):
					blob_hashes: List[str] = list(blob_hashes)
					cnt += len(blob_hashes)
					self.logger.info('Migrating blobs {} / {}'.format(cnt, total_blob_count))

					self.__migrate_blobs(session, blob_hashes, all_hash_set, processed_hash_mapping)
					session.flush_and_expunge_all()

				meta = session.get_db_meta()  # get the meta again, cuz expunge_all() was called
				meta.hash_method = self.new_hash_method.name

			self.logger.info('Syncing config and variables')
			DbAccess.sync_hash_method()
			self.config.backup.hash_method = self.new_hash_method.name

			self.logger.info('Hash method migration done, cost {}s'.format(round(time.time() - t, 2)))

		except Exception:
			self.logger.warning('Error occurs during migration, applying rollback')
			for old_hash, new_hash in processed_hash_mapping.items():
				old_path = blob_utils.get_blob_path(old_hash)
				new_path = blob_utils.get_blob_path(new_hash)
				shutil.move(new_path, old_path)
			raise
