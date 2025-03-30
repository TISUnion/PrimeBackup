import dataclasses
import logging
from typing import Optional, List

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.blob_info import BlobInfo, BlobListSummary
from prime_backup.types.units import ByteCount
from prime_backup.utils import collection_utils, misc_utils


class BlobTrashBin(List[BlobInfo]):
	def __init__(self, logger: logging.Logger):
		super().__init__()
		self.logger = logger
		self.errors: List[Exception] = []

	def make_summary(self) -> BlobListSummary:
		return BlobListSummary.of(self)

	def erase_all(self):
		for trash in self:
			try:
				trash.blob_path.unlink()
			except Exception as e:
				self.logger.error('Error erasing blob {} at {!r}'.format(trash.hash, trash.blob_path))
				self.errors.append(e)


class DeleteOrphanBlobsAction(Action[BlobListSummary]):
	def __init__(self, blob_hash_to_check: Optional[List[str]], quiet: bool = False):
		super().__init__()
		self.blob_hash_to_check = blob_hash_to_check
		if self.blob_hash_to_check is not None:
			self.blob_hash_to_check = collection_utils.deduplicated_list(self.blob_hash_to_check)
		self.quiet = quiet

	@override
	def run(self) -> BlobListSummary:
		trash_bin = BlobTrashBin(self.logger)

		if not self.quiet:
			self.logger.info('Delete orphan blobs start')
		with DbAccess.open_session() as session:
			if self.blob_hash_to_check is None:
				hashes = session.get_all_blob_hashes()
			else:
				hashes = self.blob_hash_to_check

			orphan_blob_hashes = session.filtered_orphan_blob_hashes(hashes)
			orphan_blobs = session.get_blobs(orphan_blob_hashes)

			for blob in orphan_blobs.values():
				trash_bin.append(BlobInfo.of(blob))
			session.delete_blobs(list(orphan_blobs.keys()))

		s = trash_bin.make_summary()
		trash_bin.erase_all()

		if len(errors := trash_bin.errors) > 0:
			self.logger.error('Found {} orphan blob erasing failure in total'.format(len(errors)))
			raise errors[0]

		if not self.quiet:
			self.logger.info('Delete orphan blobs done, erasing blobs (count {}, size {} / {})'.format(
				s.count, ByteCount(s.stored_size).auto_str(), ByteCount(s.raw_size).auto_str(),
			))
		return s


@dataclasses.dataclass(frozen=True)
class DeleteBackupResult:
	backup: BackupInfo
	bls: BlobListSummary


class DeleteBackupAction(Action[DeleteBackupResult]):
	def __init__(self, backup_id: int):
		super().__init__()
		self.backup_id = misc_utils.ensure_type(backup_id, int)

	@override
	def run(self) -> DeleteBackupResult:
		self.logger.info('Deleting backup #{}'.format(self.backup_id))
		with DbAccess.open_session() as session:
			backup = session.get_backup(self.backup_id)
			backup_info = BackupInfo.of(backup)
			filesets_to_check: List[schema.Fileset] = [backup.fileset_base, backup.fileset_delta]
			session.delete_backup(backup)

			# delete fileset
			deleted_file_hashes: List[str] = []
			for fileset in filesets_to_check:
				ref_cnt = session.get_fileset_associated_backup_count(fileset.id)
				self.logger.info('Pruning fileset {}, ref_cnt={}{}'.format(fileset.id, ref_cnt, ', delete it' if ref_cnt <= 0 else ''))
				if ref_cnt <= 0:
					session.delete_fileset(fileset)
					for file in session.get_fileset_files(fileset.id):
						if file.blob_hash is not None:
							deleted_file_hashes.append(file.blob_hash)
						session.delete_file(file)

		orphan_blob_cleaner = DeleteOrphanBlobsAction(deleted_file_hashes, quiet=True)
		bls = orphan_blob_cleaner.run()

		self.logger.info('Deleted backup #{} done, -{} blobs (size {} / {})'.format(
			backup_info.id, bls.count, ByteCount(bls.stored_size).auto_str(), ByteCount(bls.raw_size).auto_str(),
		))
		return DeleteBackupResult(backup_info, bls)
