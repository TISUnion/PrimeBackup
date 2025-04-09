import dataclasses
from typing import List, Set

from prime_backup.action import Action
from prime_backup.action.delete_blob_action import DeleteBlobsAction, DeleteOrphanBlobsAction
from prime_backup.action.delete_file_action import DeleteFilesStep
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.types.blob_info import BlobListSummary
from prime_backup.types.file_info import FileListSummary
from prime_backup.types.fileset_info import FilesetListSummary


class ScanAndDeleteOrphanBlobsAction(Action[BlobListSummary]):
	MAX_IN_MEMORY_OBJECTS = 10000

	def run(self) -> BlobListSummary:
		total_bls = BlobListSummary.zero()

		while True:
			limit_reached = False

			with DbAccess.open_session() as session:
				total_blob_count = session.get_blob_count()
				checking_blob_count = 0
				orphan_blob_hashes: List[str] = []
				for blobs in session.iterate_blob_batch(batch_size=500):
					checking_blob_count += len(blobs)
					if checking_blob_count % 3000 == 0 or checking_blob_count == total_blob_count:
						self.logger.info('Checking {} / {} blobs'.format(checking_blob_count, total_blob_count))
					orphan_blob_hashes.extend(session.filtered_orphan_blob_hashes([blob.hash for blob in blobs]))
					if len(orphan_blob_hashes) > self.MAX_IN_MEMORY_OBJECTS:
						limit_reached = True
						break
				if len(orphan_blob_hashes) > 0:
					self.logger.info('Found {} orphaned blobs, deleting'.format(len(orphan_blob_hashes)))
					action = DeleteBlobsAction(orphan_blob_hashes, raise_if_not_found=True)
					bls = action.run(session=session)

			if not limit_reached:
				break

			total_bls += bls
			self.logger.info('limit_reached, next round')

		if total_bls.count > 0:
			self.logger.info('Found and deleted {} orphaned blobs in total'.format(total_bls.count))
		else:
			self.logger.info('Found 0 orphaned blob in total')
		return total_bls


class ScanAndDeleteOrphanFilesAction(Action[FileListSummary]):
	MAX_IN_MEMORY_OBJECTS = 10000

	def run(self) -> FileListSummary:
		fls = FileListSummary.zero()

		while True:
			limit_reached = False

			with DbAccess.open_session() as session:
				total_file_count = session.get_file_object_count()
				checking_file_count = 0
				orphan_files: List[schema.File] = []

				for files in session.iterate_file_batch(batch_size=2000):
					checking_file_count += len(files)
					if checking_file_count % 20000 == 0 or checking_file_count == total_file_count:
						self.logger.info('Checking {} / {} file objects'.format(checking_file_count, total_file_count))
					fileset_states = session.check_filesets_existence(list({file.fileset_id for file in files}))
					for file in files:
						if fileset_states[file.fileset_id] is False:
							orphan_files.append(file)

					if len(orphan_files) > self.MAX_IN_MEMORY_OBJECTS:
						limit_reached = True
						break

				if len(orphan_files) > 0:
					self.logger.info('Found {} orphaned file objects, deleting'.format(len(orphan_files)))
					possible_orphan_blob_hashes: Set[str] = set()
					for file in orphan_files:
						session.delete_file(file)
						if file.blob_hash is not None:
							possible_orphan_blob_hashes.add(file.blob_hash)
					fls.count += len(orphan_files)

					if len(possible_orphan_blob_hashes) > 0:
						bls = DeleteOrphanBlobsAction(possible_orphan_blob_hashes).run(session=session)
						fls.blob_summary += bls

			if not limit_reached:
				break
			self.logger.info('limit_reached, next round')

		if fls.count > 0:
			self.logger.info('Found and deleted {} orphaned file objects in total'.format(fls.count))
		else:
			self.logger.info('Found 0 orphaned file object in total')
		return fls


class ScanAndDeleteOrphanFilesetsAction(Action[FilesetListSummary]):
	MAX_IN_MEMORY_OBJECTS = 10000

	def run(self) -> FilesetListSummary:
		fsls = FilesetListSummary.zero()

		while True:
			limit_reached = False

			with DbAccess.open_session() as session:
				filesets = session.list_fileset()
				self.logger.info('Checking {} fileset objects'.format(len(filesets)))

				orphan_fileset_ids = set(session.filtered_orphan_fileset_ids([fileset.id for fileset in filesets]))
				self.logger.info('Found {} orphan fileset'.format(len(orphan_fileset_ids)))

				files_to_delete: List[schema.File] = []
				for fileset in filesets:
					if fileset.id not in orphan_fileset_ids:
						continue
					files_to_delete_for_this_fileset = session.get_fileset_files(fileset.id)
					files_to_delete.extend(files_to_delete_for_this_fileset)
					session.delete_fileset(fileset)

					fsls.count += len(files_to_delete_for_this_fileset)

					if len(filesets) + len(files_to_delete) > self.MAX_IN_MEMORY_OBJECTS:
						limit_reached = True
						break

				if len(files_to_delete) > 0:
					self.logger.info('Deleting {} file objects from {} orphan filesets'.format(len(files_to_delete), len(orphan_fileset_ids)))
					fls = DeleteFilesStep(session, files_to_delete).run()
					fsls.file_summary += fls

			if not limit_reached:
				break

			self.logger.info('limit_reached, next round')

		if fsls.count > 0:
			self.logger.info('Found and deleted {} orphaned fileset objects with {} files in total'.format(fsls.count, fsls.file_summary.count))
		else:
			self.logger.info('Found 0 orphaned fileset object in total')
		return fsls


@dataclasses.dataclass(frozen=True)
class ScanAndDeleteOrphanObjectsResult:
	orphan_blob_count: int
	orphan_file_count: int
	orphan_fileset_count: int

	@property
	def total_orphan_count(self) -> int:
		return self.orphan_blob_count + self.orphan_file_count + self.orphan_fileset_count


class ScanAndDeleteOrphanObjectsAction(Action[ScanAndDeleteOrphanObjectsResult]):
	def run(self) -> ScanAndDeleteOrphanObjectsResult:
		self.logger.info('Scanning orphan objects')
		orphan_blob_summary = ScanAndDeleteOrphanBlobsAction().run()
		orphan_file_summary = ScanAndDeleteOrphanFilesAction().run()
		orphan_fileset_summary = ScanAndDeleteOrphanFilesetsAction().run()
		result = ScanAndDeleteOrphanObjectsResult(
			orphan_blob_count=orphan_blob_summary.count,
			orphan_file_count=orphan_file_summary.count,
			orphan_fileset_count=orphan_fileset_summary.count,
		)

		if result.total_orphan_count > 0:
			self.logger.info('Found and deleted {} orphan blobs, {} orphan file objects (with {} blobs), {} orphan fileset objects (with {} files and {} blobs) in total'.format(
				orphan_blob_summary.count,
				orphan_file_summary.count, orphan_file_summary.blob_summary.count,
				orphan_fileset_summary.count, orphan_fileset_summary.file_summary.count, orphan_fileset_summary.file_summary.blob_summary.count,
			))
		else:
			self.logger.info('Found 0 orphan object, everything looks good')
		return result
