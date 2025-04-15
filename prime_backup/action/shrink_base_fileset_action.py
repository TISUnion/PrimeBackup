from typing import Dict, Set, List

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.action.delete_blob_action import DeleteOrphanBlobsAction
from prime_backup.action.list_fileset_action import ListFilesetAction
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.values import FileRole
from prime_backup.exceptions import PrimeBackupError, FilesetNotFound
from prime_backup.types.blob_info import BlobListSummary
from prime_backup.types.file_info import FileListSummary
from prime_backup.types.units import ByteCount


class NotBaseFileset(PrimeBackupError):
	pass


_DEBUG_LOG = False


class ShrinkBaseFilesetAction(Action[FileListSummary]):
	def __init__(self, base_fileset_id: int):
		super().__init__()
		self.base_fileset_id = base_fileset_id

	@override
	def run(self) -> FileListSummary:
		self.logger.info('Shrinking base fileset {}'.format(self.base_fileset_id))
		deleted_file_hashes: Set[str] = set()
		fls = FileListSummary.zero()

		with DbAccess.open_session() as session:
			base_fileset: schema.Fileset = session.get_fileset(self.base_fileset_id)
			if base_fileset.base_id != 0:
				raise NotBaseFileset('Fileset {} with base_id {} is not a base fileset'.format(self.base_fileset_id, base_fileset.base_id))
			base_files_by_path: Dict[str, schema.File] = {file.path: file for file in session.get_fileset_files(self.base_fileset_id)}
			base_path_in_used = {path: False for path in base_files_by_path.keys()}
			base_path_in_used_count = 0

			delta_filesets: List[schema.Fileset] = session.get_delta_filesets_for_base_fileset(self.base_fileset_id)
			if _DEBUG_LOG:
				self.logger.info('DBG: delta_filesets len {}'.format(len(delta_filesets)))

			delta_file: schema.File
			for delta_fileset in delta_filesets:
				delta_in_used_paths: Set[str] = set(base_path_in_used.keys())
				for delta_file in session.get_fileset_files(delta_fileset.id):
					if delta_file.role in [FileRole.delta_override.value, FileRole.delta_remove.value]:
						try:
							delta_in_used_paths.remove(delta_file.path)
						except KeyError:
							self.logger.warning('delta file path not in base file paths? base fileset id {}, delta fileset id {}, delta file {}'.format(delta_fileset.id, delta_fileset.id, repr(delta_file)))
				for delta_in_used_path in delta_in_used_paths:
					if not base_path_in_used[delta_in_used_path]:
						base_path_in_used_count += 1
					base_path_in_used[delta_in_used_path] = True

				# fast break if `all(base_path_in_used.values())`
				if base_path_in_used_count == len(base_path_in_used):
					break

			if _DEBUG_LOG:
				self.logger.info('DBG: base_path_in_used')
				for k, v in base_path_in_used.items():
					self.logger.info(f'DBG:   {k.ljust(80)}: {v}')

			unused_base_files: Dict[str, schema.File] = {}
			for path, file in base_files_by_path.items():
				if base_path_in_used[path] is False:
					unused_base_files[path] = file

			if _DEBUG_LOG:
				self.logger.info('DBG: unused_base_files')
				for k in unused_base_files:
					self.logger.info(f'DBG:   {k}')

			if len(unused_base_files) > 0:
				self.logger.info('Found {} unused files in base fileset {}, shrinking'.format(len(unused_base_files), self.base_fileset_id))
				for unused_base_file in unused_base_files.values():
					if unused_base_file.blob_hash is not None:
						deleted_file_hashes.add(unused_base_file.blob_hash)

					base_fileset.file_count -= 1
					base_fileset.file_object_count -= 1
					base_fileset.file_raw_size_sum -= unused_base_file.blob_raw_size or 0
					base_fileset.file_stored_size_sum -= unused_base_file.blob_stored_size or 0

				files_to_delete: List[schema.File] = []
				files_to_delete.extend(unused_base_files.values())
				for delta_fileset in delta_filesets:
					for delta_file in session.get_fileset_files(delta_fileset.id):
						if (unused_base_file := unused_base_files.get(delta_file.path)) is None:
							continue
						if delta_file.role in [FileRole.delta_override.value, FileRole.delta_remove.value]:
							if delta_file.role == FileRole.delta_override.value:
								if _DEBUG_LOG:
									self.logger.info(f'DBG: fileset {delta_fileset.id}, {delta_file.path}: override -> add')
								# override -> add
								delta_file.role = FileRole.delta_add
								delta_fileset.file_count += 1
							else:
								if _DEBUG_LOG:
									self.logger.info(f'DBG: fileset {delta_fileset.id}, {delta_file.path}: remove -> X')
								# remove -> X
								files_to_delete.append(delta_file)
								delta_fileset.file_count += 1
								delta_fileset.file_object_count -= 1
							delta_fileset.file_raw_size_sum += unused_base_file.blob_raw_size or 0
							delta_fileset.file_stored_size_sum += unused_base_file.blob_stored_size or 0

				self.logger.info('Deleting {} file objects'.format(len(files_to_delete)))
				fls.count += len(files_to_delete)
				for file_to_delete in files_to_delete:
					session.delete_file(file_to_delete)

			if _DEBUG_LOG:
				self.logger.info('DBG: deleted_file_hashes len {}'.format(len(deleted_file_hashes)))
			if len(deleted_file_hashes) > 0:
				orphan_blob_cleaner = DeleteOrphanBlobsAction(deleted_file_hashes)
				bls = orphan_blob_cleaner.run(session=session)
			else:
				bls = BlobListSummary.zero()
			fls.blob_summary = bls

		self.logger.info('Shrink base fileset {} done, -{} blobs (size {} / {})'.format(
			self.base_fileset_id, bls.count, ByteCount(bls.stored_size).auto_str(), ByteCount(bls.raw_size).auto_str(),
		))
		return fls


class ShrinkAllBaseFilesetsAction(Action[FileListSummary]):
	@override
	def run(self) -> FileListSummary:
		filesets = ListFilesetAction(is_base=True).run()
		fls_total = FileListSummary.zero()

		self.logger.info('Shrinking {} base filesets'.format(len(filesets)))
		for fileset in filesets:
			try:
				fls = ShrinkBaseFilesetAction(fileset.id).run()
			except (FilesetNotFound, NotBaseFileset):
				continue

			fls_total += fls
			if fls.count > 0:
				self.logger.debug('ShrinkBaseFilesetAction for {} done: {}'.format(fileset.id, fls))
			else:
				self.logger.debug('ShrinkBaseFilesetAction for {} done, noting to do'.format(fileset.id))

		self.logger.info('Shrank {} base filesets: {}'.format(len(filesets), fls_total))
		return fls_total
