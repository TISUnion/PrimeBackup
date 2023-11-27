import os
from pathlib import Path
from typing import Optional, NamedTuple, List

from xbackup.db import schema
from xbackup.db.access import DbAccess
from xbackup.db.session import DbSession
from xbackup.task.task import Task
from xbackup.task.types.backup_info import BackupInfo
from xbackup.utils import collection_utils, blob_utils


class BlobTrashBin:
	class Trash(NamedTuple):
		hash: str
		raw_size: int
		blob_path: Path

	class Summary(NamedTuple):
		count: int
		raw_size_sum: int
		actual_size_sum: int

	def __init__(self):
		self.trashes: List[BlobTrashBin.Trash] = []

	def add(self, blob: schema.Blob):
		blob_path = blob_utils.get_blob_path(blob.hash)
		self.trashes.append(self.Trash(blob.hash, blob.size, blob_path))

	def make_summary(self) -> Summary:
		raw_size_sum, actual_size_sum = 0, 0
		for trash in self.trashes:
			raw_size_sum += trash.raw_size
			actual_size_sum += os.stat(trash.blob_path).st_size
		return self.Summary(len(self.trashes), raw_size_sum, actual_size_sum)

	def erase_all(self):
		for trash in self.trashes:
			trash.blob_path.unlink()


class DeleteOrphanBlobsTask(Task):
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
				trash_bin.add(blob)
			self.logger.info('orphan blob added to trashbin, cnt %s', len(t))
			session.delete_blobs(list(t.keys()))
			self.logger.info('orphan blob deleted, cnt %s', len(t))

		summary = trash_bin.make_summary()
		trash_bin.erase_all()
		self.logger.info('Delete blobs done, erasing blobs (count {}, size {} / {})'.format(summary.count, summary.actual_size_sum, summary.raw_size_sum))


class DeleteBackupTask(Task):
	def __init__(self, backup_id: int):
		super().__init__()
		self.backup_id = backup_id
		self.orphan_blob_cleaner: Optional[DeleteOrphanBlobsTask] = None

	def run(self) -> BackupInfo:
		self.logger.info('Deleting backup {}'.format(self.backup_id))
		with DbAccess.open_session() as session:
			backup = session.get_backup_or_throw(self.backup_id)
			info = BackupInfo.of(backup)
			self.__delete_backup(session, backup)
		self.orphan_blob_cleaner.run()
		return info

	def __delete_backup(self, session: DbSession, backup: schema.Backup):
		hashes = []
		for file in backup.files:
			if file.blob_hash is not None:
				hashes.append(file.blob_hash)
			session.delete_file(file)
		session.delete_backup(backup)
		self.orphan_blob_cleaner = DeleteOrphanBlobsTask(hashes)
