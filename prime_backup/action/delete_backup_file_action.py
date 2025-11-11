import stat
from pathlib import Path
from typing import List, Dict, Set

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

	@override
	def run(self) -> BlobListSummary:
		self.logger.info('Deleting file {!r} in backup #{}'.format(self.file_path, self.backup_id))
		with DbAccess.open_session() as session:
			backup = session.get_backup(self.backup_id)
			backup_info = BackupInfo.of(backup)
			fileset_id_base: int = backup_info.fileset_id_base
			fileset_id_delta: int = backup_info.fileset_id_delta
			fileset_delta: schema.Fileset = session.get_fileset(backup.fileset_id_delta)
			files_base: Dict[str, schema.File] = {file.path: file for file in session.get_fileset_files(backup.fileset_id_base)}
			files_delta: Dict[str, schema.File] = {file.path: file for file in session.get_fileset_files(backup.fileset_id_delta)}
			files_merged: Dict[str, schema.File] = {file.path: file for file in session.merge_fileset_files(files_base.values(), files_delta.values())}

			if (file_target := files_merged.get(self.file_path)) is None:
				raise BackupFileNotFound(self.backup_id, self.file_path)
			self.logger.debug('Old db data: fileset_delta={!r}, backup={!r}'.format(fileset_delta, backup))

			files_to_delete: List[schema.File] = [file_target]
			if target_is_dir := stat.S_ISDIR(file_target.mode):
				# collect contents inside the directory recursively
				for file in files_merged.values():
					if file.path.startswith(self.file_path + '/'):
						files_to_delete.append(file)
			self.logger.info('Target file to delete {!r} is dir: {}, all files_to_delete[:10](size={}): {}'.format(self.file_path, target_is_dir, len(files_to_delete), files_to_delete[:10]))

			deleted_blob_hashes: Set[str] = set()
			for file_existing in files_to_delete:
				file_base = files_base.get(file_existing.path)
				file_delta = files_delta.get(file_existing.path)
				if file_existing is file_target:
					self.logger.info('Base fileset id {} target file {!r}'.format(fileset_id_base, file_base))
					self.logger.info('Delta fileset id {} target file {!r}'.format(fileset_id_delta, file_delta))

				if file_existing.fileset_id == fileset_id_delta:
					# remove the existing one in the delta fileset first
					self.logger.debug('File exists in the delta fileset {}, delete it first'.format(fileset_id_delta))
					if file_existing.role == FileRole.delta_add.value:
						if file_base is not None:
							raise AssertionError('file {!r} in delta fileset {} has role delta_add, but it also exists in the base fileset {}: {!r}'.format(file_existing, fileset_id_delta, fileset_id_base, file_base))
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
						deleted_blob_hashes.add(file_existing.blob_hash)
						self.logger.debug('Added blob_hash {} to deleted_blob_hashes'.format(file_existing.blob_hash))
					session.delete_file(file_existing)
				elif file_existing.fileset_id != fileset_id_base:
					raise AssertionError('unexpected fileset id {} for file {!r}'.format(file_existing.fileset_id, file_existing))

				if file_base is not None:
					self.logger.debug('File {!r} exists in the base fileset {}, create a delta_remove file'.format(file_existing.path, backup.fileset_id_delta))
					session.add(session.create_delta_remove_file(path=file_existing.path, fileset_id=backup.fileset_id_delta))
					fileset_delta.file_count -= 1
					fileset_delta.file_object_count += 1
					fileset_delta.file_raw_size_sum -= file_base.blob_raw_size or 0
					fileset_delta.file_stored_size_sum -= file_base.blob_stored_size or 0
					backup.file_count -= 1
					backup.file_raw_size_sum -= file_base.blob_raw_size or 0
					backup.file_stored_size_sum -= file_base.blob_stored_size or 0

			self.logger.debug('New db data: fileset_delta={!r}, backup={!r}'.format(fileset_delta, backup))
			if len(deleted_blob_hashes) > 0:
				self.logger.info('Running DeleteOrphanBlobsAction for hashes[:10](size={}) {}'.format(len(deleted_blob_hashes), list(deleted_blob_hashes)[:10]))
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
