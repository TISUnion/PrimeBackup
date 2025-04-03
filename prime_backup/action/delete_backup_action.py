import dataclasses
from typing import List, Set

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.action.delete_blob_action import DeleteOrphanBlobsAction
from prime_backup.action.shrink_base_fileset_action import ShrinkBaseFilesetAction
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.blob_info import BlobListSummary
from prime_backup.types.units import ByteCount
from prime_backup.utils import misc_utils


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
		base_fileset_alive = True
		with DbAccess.open_session() as session:
			backup = session.get_backup(self.backup_id)
			backup_info = BackupInfo.of(backup)
			filesets_to_check: List[schema.Fileset] = [backup.fileset_base, backup.fileset_delta]
			session.delete_backup(backup)

			# delete fileset
			deleted_file_hashes: Set[str] = set()
			for fileset in filesets_to_check:
				ref_cnt = session.get_fileset_associated_backup_count(fileset.id)
				self.logger.info('Pruning fileset {}, ref_cnt={}{}'.format(fileset.id, ref_cnt, ', delete it' if ref_cnt <= 0 else ''))
				if ref_cnt <= 0:
					if fileset.id == backup_info.fileset_id_base:
						base_fileset_alive = False
					session.delete_fileset(fileset)
					for file in session.get_fileset_files(fileset.id):
						if file.blob_hash is not None:
							deleted_file_hashes.add(file.blob_hash)
						session.delete_file(file)

			orphan_blob_cleaner = DeleteOrphanBlobsAction(deleted_file_hashes)
			bls = orphan_blob_cleaner.run(session=session)

		if base_fileset_alive:
			self.logger.info('Shrinking base fileset {} since it''s still alive'.format(backup_info.fileset_id_base))
			sbf_action = ShrinkBaseFilesetAction(backup_info.fileset_id_base)
			sbf_action.run()

		self.logger.info('Deleted backup #{} done, -{} blobs (size {} / {})'.format(
			backup_info.id, bls.count, ByteCount(bls.stored_size).auto_str(), ByteCount(bls.raw_size).auto_str(),
		))
		return DeleteBackupResult(backup_info, bls)
