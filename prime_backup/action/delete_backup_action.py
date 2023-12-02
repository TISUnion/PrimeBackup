from typing import Optional, NamedTuple, List

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.blob_info import BlobInfo
from prime_backup.types.units import ByteCount
from prime_backup.utils import collection_utils, misc_utils


class BlobTrashBin:
	class Summary(NamedTuple):
		count: int
		raw_size_sum: int
		stored_size_sum: int

	def __init__(self):
		self.trashes: List[BlobInfo] = []

	def add(self, blob: BlobInfo):
		self.trashes.append(blob)

	def make_summary(self) -> Summary:
		raw_size_sum, stored_size_sum = 0, 0
		for trash in self.trashes:
			raw_size_sum += trash.raw_size
			stored_size_sum += trash.stored_size
		return self.Summary(len(self.trashes), raw_size_sum, stored_size_sum)

	def erase_all(self):
		for trash in self.trashes:
			trash.blob_path.unlink()


class DeleteOrphanBlobsAction(Action):
	def __init__(self, blob_hash_to_check: Optional[List[str]]):
		super().__init__()
		self.blob_hash_to_check = blob_hash_to_check
		if self.blob_hash_to_check is not None:
			self.blob_hash_to_check = collection_utils.deduplicated_list(self.blob_hash_to_check)

	def run(self):
		trash_bin = BlobTrashBin()

		self.logger.info('Delete blobs start')
		with DbAccess.open_session() as session:
			if self.blob_hash_to_check is None:
				hashes = session.get_all_blob_hashes()
			else:
				hashes = self.blob_hash_to_check

			self.logger.info('blob hash collected, cnt %s', len(hashes))
			orphan_blob_hashes = session.filtered_orphan_blob_hashes(hashes)
			self.logger.info('orphan hash collected, cnt %s', len(hashes))
			t = session.get_blobs(orphan_blob_hashes)
			self.logger.info('orphan blob collected, cnt %s', len(t))

			for blob in t.values():
				trash_bin.add(BlobInfo.of(blob))
			self.logger.info('orphan blob added to trashbin, cnt %s', len(t))
			session.delete_blobs(list(t.keys()))
			self.logger.info('orphan blob deleted, cnt %s', len(t))

		summary = trash_bin.make_summary()
		trash_bin.erase_all()
		self.logger.info('Delete blobs done, erasing blobs (count {}, size {} / {})'.format(
			summary.count, ByteCount(summary.stored_size_sum).auto_str(), ByteCount(summary.raw_size_sum).auto_str(),
		))


class DeleteBackupAction(Action):
	def __init__(self, backup_id: int):
		super().__init__()
		self.backup_id = misc_utils.ensure_type(backup_id, int)

	def run(self) -> BackupInfo:
		self.logger.info('Deleting backup {}'.format(self.backup_id))
		with DbAccess.open_session() as session:
			backup = session.get_backup(self.backup_id)
			info = BackupInfo.of(backup)

			hashes = []
			for file in backup.files:
				if file.blob_hash is not None:
					hashes.append(file.blob_hash)
				session.delete_file(file)
			session.delete_backup(backup)

			orphan_blob_cleaner = DeleteOrphanBlobsAction(hashes)

		orphan_blob_cleaner.run()
		return info
