import stat
from pathlib import Path
from typing import List, Optional

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.action.delete_blob_action import DeleteOrphanBlobsAction
from prime_backup.action.shrink_base_fileset_action import ShrinkBaseFilesetAction
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.values import FileRole
from prime_backup.exceptions import BackupFileNotFound
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.blob_info import BlobListSummary
from prime_backup.types.units import ByteCount
from prime_backup.utils.path_like import PathLike


class DeleteBackupFileAction(Action[BlobListSummary]):
	def __init__(self, backup_id: int, file_path: PathLike):
		super().__init__()
		self.backup_id = backup_id
		self.file_path = Path(file_path).as_posix()

	def __locate_file(self, files: List[schema.File]) -> Optional[schema.File]:
		for file in files:
			if file.path == self.file_path:
				return file
		return None

	@override
	def run(self) -> BlobListSummary:
		self.logger.info('Deleting file {!r} in backup #{}'.format(self.file_path, self.backup_id))
		with DbAccess.open_session() as session:
			backup = session.get_backup(self.backup_id)
			backup_info = BackupInfo.of(backup)
			fileset_id_base = backup_info.fileset_id_base
			fileset_id_delta = backup_info.fileset_id_delta
			fileset_delta = session.get_fileset(backup.fileset_id_delta)
			files_base = session.get_fileset_files(backup.fileset_id_base)
			files_delta = session.get_fileset_files(backup.fileset_id_delta)

			if (file_existing := self.__locate_file(session.merge_fileset_files(files_base, files_delta))) is None:
				raise BackupFileNotFound(self.backup_id, self.file_path)
			if stat.S_ISDIR(file_existing.mode):
				raise NotImplementedError('file is dir, not supported yet')
			file_base = self.__locate_file(files_base)
			file_delta = self.__locate_file(files_delta)
			self.logger.info('Base fileset id {} file {!r}'.format(fileset_id_base, file_base))
			self.logger.info('Delta fileset id {} file {!r}'.format(fileset_id_delta, file_delta))
			self.logger.debug('Old delta fileset data: {!r}'.format(fileset_delta))

			deleted_blob_hashes: List[str] = []
			if file_existing.fileset_id == fileset_id_delta:
				# remove the existing one in the delta fileset first
				self.logger.debug('File exists in the delta fileset {}, delete it first'.format(fileset_id_delta))
				if file_existing.role == FileRole.delta_add.value:
					if file_base is not None:
						raise AssertionError('file {!r} in delta fileset {} has role delta_add, but it also exists in the base fileset {}'.format(file_existing, fileset_id_delta, fileset_id_base))
					fileset_delta.file_count -= 1
					fileset_delta.file_object_count -= 1
					fileset_delta.file_raw_size_sum -= file_existing.blob_raw_size or 0
					fileset_delta.file_stored_size_sum -= file_existing.blob_stored_size or 0
					backup.file_count -= 1
					backup.file_raw_size_sum -= file_existing.blob_raw_size or 0
					backup.file_stored_size_sum -= file_existing.blob_stored_size or 0
				elif file_existing.role == FileRole.delta_override.value:
					if file_base is None:
						raise AssertionError('file {!r} in delta fileset {} has role delta_override, but it does not exist in the base fileset {}'.format(file_existing, fileset_id_delta, fileset_id_base))
					fileset_delta.file_object_count -= 1
					fileset_delta.file_raw_size_sum -= (file_existing.blob_raw_size or 0) - (file_base.blob_raw_size or 0)
					fileset_delta.file_stored_size_sum -= (file_existing.blob_stored_size or 0) - (file_base.blob_stored_size or 0)
					backup.file_raw_size_sum -= (file_existing.blob_raw_size or 0) - (file_base.blob_raw_size or 0)
					backup.file_stored_size_sum -= (file_existing.blob_stored_size or 0) - (file_base.blob_stored_size or 0)
				else:
					raise AssertionError('unexpected delta file role {} for file {!r}'.format(file_existing.role, file_existing))
				if file_existing.blob_hash is not None:
					deleted_blob_hashes.append(file_existing.blob_hash)
					self.logger.debug('Added blob_hash {} to deleted_blob_hashes'.format(file_existing.blob_hash))
				session.delete_file(file_existing)
			elif file_existing.fileset_id != fileset_id_base:
				raise AssertionError('unexpected fileset id {} for file {!r}'.format(file_existing.fileset_id, file_existing))

			if file_base is not None:
				self.logger.debug('File exists in the base fileset {}, create a delta_remove file'.format(backup.fileset_id_delta))
				session.add(session.create_delta_remove_file(path=self.file_path, fileset_id=backup.fileset_id_delta))
				fileset_delta.file_count -= 1
				fileset_delta.file_object_count += 1
				fileset_delta.file_raw_size_sum -= file_base.blob_raw_size or 0
				fileset_delta.file_stored_size_sum -= file_base.blob_stored_size or 0
				backup.file_count -= 1
				backup.file_raw_size_sum -= file_base.blob_raw_size or 0
				backup.file_stored_size_sum -= file_base.blob_stored_size or 0

			self.logger.debug('New delta fileset data: {!r}'.format(fileset_delta))
			if len(deleted_blob_hashes) > 0:
				self.logger.info('Running DeleteOrphanBlobsAction for hashes {}'.format(deleted_blob_hashes))
				bls = DeleteOrphanBlobsAction(deleted_blob_hashes).run(session=session)
			else:
				bls = BlobListSummary.zero()

		self.logger.info('Running ShrinkBaseFilesetAction for the base fileset {}'.format(fileset_id_base))
		fls = ShrinkBaseFilesetAction(fileset_id_base).run()
		bls += fls.blob_summary

		self.logger.info('Deleting file {!r} in backup #{}, -{} blobs (size {} / {})'.format(
			self.file_path, self.backup_id,
			bls.count, ByteCount(bls.stored_size).auto_str(), ByteCount(bls.raw_size).auto_str(),
		))
		return bls
