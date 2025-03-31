from typing import List, Set

from prime_backup.action import Action
from prime_backup.action.delete_blob_action import DeleteBlobsAction, DeleteOrphanBlobsAction
from prime_backup.action.delete_file_action import DeleteFilesStep
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.types.blob_info import BlobListSummary


class RemoveOrphanBlobsAction(Action[None]):
	MAX_IN_MEMORY_OBJECTS = 10000

	def run(self) -> BlobListSummary:
		total_bls = BlobListSummary.zero()

		while True:
			limit_reached = False

			with DbAccess.open_session() as session:
				total_blob_count = session.get_blob_count()
				orphan_blob_hashes: List[str] = []
				for blobs in session.iterate_blob_batch(batch_size=500):
					self.logger.info('Checking {} / {} blobs'.format(len(blobs), total_blob_count))
					orphan_blob_hashes.extend(session.filtered_orphan_blob_hashes([blob.hash for blob in blobs]))
					if len(orphan_blob_hashes) > self.MAX_IN_MEMORY_OBJECTS:
						limit_reached = True
						break
				action = DeleteBlobsAction(orphan_blob_hashes, raise_if_not_found=True)
				bls = action.run(session=session)

			if not limit_reached:
				break

			total_bls = total_bls + bls
			self.logger.info('limit_reached, next round')

		return total_bls


class RemoveOrphanFilesAction(Action[None]):
	MAX_IN_MEMORY_OBJECTS = 10000

	def run(self) -> int:
		total_deleted_count = 0

		while True:
			limit_reached = False

			with DbAccess.open_session() as session:
				total_file_count = session.get_file_object_count()
				orphan_files: List[schema.File] = []

				for files in session.iterate_file_batch(batch_size=3000):
					self.logger.info('Checking {} / {} file objects'.format(len(files), total_file_count))
					fileset_states = session.check_filesets_existence(list({file.fileset_id for file in files}))
					for file in files:
						if fileset_states[file.fileset_id] is False:
							orphan_files.append(file)

					if len(orphan_files) > self.MAX_IN_MEMORY_OBJECTS:
						limit_reached = True
						break

				possible_orphan_blob_hashes: Set[str] = set()
				for file in orphan_files:
					session.delete_file(file)
					if file.blob_hash is not None:
						possible_orphan_blob_hashes.add(file.blob_hash)
				total_deleted_count += len(orphan_files)

				DeleteOrphanBlobsAction(possible_orphan_blob_hashes).run(session=session)

			if not limit_reached:
				break
			self.logger.info('limit_reached, next round')

		return total_deleted_count


class RemoveOrphanFilesetsAction(Action[None]):
	MAX_IN_MEMORY_OBJECTS = 10000

	def run(self) -> None:
		while True:
			limit_reached = False

			with DbAccess.open_session() as session:
				filesets = session.list_fileset()
				orphan_fileset_ids = set(session.filtered_orphan_fileset_ids([fileset.id for fileset in filesets]))
				files_to_delete: List[schema.File] = []
				for fileset in filesets:
					if fileset.id not in orphan_fileset_ids:
						continue
					files_to_delete.extend(session.get_fileset_files(fileset.id))
					session.delete_fileset(fileset)

					if len(filesets) + len(files_to_delete) > self.MAX_IN_MEMORY_OBJECTS:
						limit_reached = True
						break

				DeleteFilesStep(session, files_to_delete).run()

			if not limit_reached:
				break

			self.logger.info('limit_reached, next round')


class RemoveOrphanObjectsAction(Action[None]):
	def run(self) -> None:
		self.logger.info('Removing orphan objects start')
		RemoveOrphanFilesetsAction().run()
		RemoveOrphanFilesAction().run()
		RemoveOrphanBlobsAction().run()
		self.logger.info('Removing orphan objects done')
