import dataclasses
from typing import List

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.action.delete_blob_action import DeleteBlobsAction
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.blob_info import BlobListSummary
from prime_backup.types.units import ByteCount
from prime_backup.utils import collection_utils, misc_utils


class _CheckAndDeleteOrphanBlobsAction(Action[BlobListSummary]):
	def __init__(self, blob_hashes_to_check: List[str], quiet: bool = False):
		super().__init__()
		self.blob_hashes_to_check = collection_utils.deduplicated_list(blob_hashes_to_check)
		self.quiet = quiet

	@override
	def run(self) -> BlobListSummary:
		if not self.quiet:
			self.logger.info('Delete orphan blobs start')

		with DbAccess.open_session() as session:
			orphan_blob_hashes = session.filtered_orphan_blob_hashes(self.blob_hashes_to_check)

		if len(orphan_blob_hashes) > 0:
			action = DeleteBlobsAction(orphan_blob_hashes, raise_if_not_found=True)
			bls = action.run()
		else:
			bls = BlobListSummary.zero()

		if not self.quiet:
			self.logger.info('Delete orphan blobs done, erasing blobs (count {}, size {} / {})'.format(
				bls.count, ByteCount(bls.stored_size).auto_str(), ByteCount(bls.raw_size).auto_str(),
			))
		return bls


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

		orphan_blob_cleaner = _CheckAndDeleteOrphanBlobsAction(deleted_file_hashes, quiet=True)
		bls = orphan_blob_cleaner.run()

		self.logger.info('Deleted backup #{} done, -{} blobs (size {} / {})'.format(
			backup_info.id, bls.count, ByteCount(bls.stored_size).auto_str(), ByteCount(bls.raw_size).auto_str(),
		))
		return DeleteBackupResult(backup_info, bls)
