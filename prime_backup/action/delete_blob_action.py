import contextlib
import logging
from typing import List, Dict, Optional, Collection

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.action.delete_chunk_group_action import DeleteOrphanChunkGroupsAction
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.db.values import BlobStorageMethod
from prime_backup.exceptions import BlobHashNotFound, BlobIdNotFound
from prime_backup.types.blob_info import BlobInfo, BlobListSummary, BlobDeltaSummary
from prime_backup.types.chunk_info import ChunkListSummary
from prime_backup.utils import collection_utils


class _DirectBlobTrashBin:
	def __init__(self, logger: logging.Logger):
		self.trash_blobs: List[BlobInfo] = []
		self.logger = logger
		self.errors: List[Exception] = []

	def add(self, blob_info: BlobInfo):
		self.trash_blobs.append(blob_info)

	def make_summary(self) -> BlobListSummary:
		return BlobListSummary.of(self.trash_blobs)

	def erase_all(self):
		for trash in self.trash_blobs:
			try:
				trash.blob_file_path.unlink(missing_ok=True)
			except Exception as e:
				self.logger.error('Error erasing blob {} at {!r}'.format(trash.hash, trash.blob_file_path))
				self.errors.append(e)


class DeleteBlobsAction(Action[BlobDeltaSummary]):
	def __init__(self, *, ids: Collection[int] = (), hashes: Collection[str] = (), raise_if_not_found: bool = True):
		super().__init__()
		self.blob_ids = collection_utils.deduplicated_list(ids)
		self.blob_hashes = collection_utils.deduplicated_list(hashes)
		self.raise_if_not_found = raise_if_not_found

	@override
	def run(self, *, session: Optional[DbSession] = None) -> BlobDeltaSummary:
		"""
		:param session: If provided, use this session for DB operations.
		NOTES: `session.commit()` will be called, so it's better to call this at the end of a `DbAccess.open_session()` block
		"""
		direct_blob_trash_bin = _DirectBlobTrashBin(self.logger)

		chunk_summary = ChunkListSummary.zero()
		with contextlib.ExitStack() as es:
			if session is None:
				session = es.enter_context(DbAccess.open_session())

			all_to_delete_blobs = self.__collect_blobs_to_delete(session)
			for blob in all_to_delete_blobs.values():
				if blob.storage_method == BlobStorageMethod.direct:
					direct_blob_trash_bin.add(blob)

			if len(all_to_delete_blobs) > 0:
				# 1. delete Blob-ChunkGroup bindings
				# 2. delete blobs
				# 3. check orphan chunk groups
				affected_chunk_group_ids: Dict[int, None] = {}  # ordered set
				chunked_blob_ids: List[int] = [
					blob.id for blob in all_to_delete_blobs.values()
					if blob.storage_method == BlobStorageMethod.chunked
				]
				if len(chunked_blob_ids) > 0:
					bindings = session.get_blob_chunk_group_bindings_for_blobs(chunked_blob_ids)
					for bcg in bindings:
						affected_chunk_group_ids[bcg.chunk_group_id] = None
					session.delete_blob_chunk_group_bindings_for_blobs(chunked_blob_ids)

				session.delete_blobs_by_ids(list(all_to_delete_blobs.keys()))
				if len(affected_chunk_group_ids) > 0:
					chunk_group_summary = DeleteOrphanChunkGroupsAction(ids=affected_chunk_group_ids.keys()).run(session=session)
				else:
					session.commit()
			else:
				session.commit()

		direct_blob_summary = direct_blob_trash_bin.make_summary()
		direct_blob_trash_bin.erase_all()
		self.logger.debug('Deleted {} direct blobs: {}'.format(direct_blob_summary.count, direct_blob_summary))

		if len(errors := direct_blob_trash_bin.errors) > 0:
			self.logger.error('Found {} blob erasing failures in total'.format(len(errors)))
			raise errors[0]

		return BlobDeltaSummary.of(list(all_to_delete_blobs.values()), chunk_group_summary.chunk_summary)

	def __collect_blobs_to_delete(self, session: DbSession) -> Dict[int, BlobInfo]:
		self_blob_ids_set = set(self.blob_ids)
		self_blob_hashes_set = set(self.blob_hashes)
		all_to_delete_blobs: Dict[int, BlobInfo] = {}

		blobs_by_id: Dict[int, Optional[schema.Blob]] = session.get_blobs_by_ids(self.blob_ids)
		for blob_id, blob in blobs_by_id.items():
			if blob is None:
				if self.raise_if_not_found:
					raise BlobIdNotFound(blob_id)
				else:
					self.logger.warning('Blob with id {} does not exist'.format(blob_id))
					continue
			if blob_id not in self_blob_ids_set:
				raise AssertionError('got unexpected blob id {!r}, should be in {}'.format(blob_id, self_blob_ids_set))
			all_to_delete_blobs[blob.id] = BlobInfo.of(blob)

		blobs_by_hash: Dict[str, Optional[schema.Blob]] = session.get_blobs_by_hashes_opt(self.blob_hashes)
		for blob_hash, blob in blobs_by_hash.items():
			if blob is None:
				if self.raise_if_not_found:
					raise BlobHashNotFound(blob_hash)
				else:
					self.logger.warning('Blob with hash {} does not exist'.format(blob_hash))
					continue
			if blob_hash not in self_blob_hashes_set:
				raise AssertionError('got unexpected blob hash {!r}, should be in {}'.format(blob_hash, self_blob_hashes_set))
			all_to_delete_blobs[blob.id] = BlobInfo.of(blob)

		return all_to_delete_blobs


class DeleteOrphanBlobsAction(Action[BlobDeltaSummary]):
	def __init__(self, blob_hashes_to_check: Collection[str]):
		super().__init__()
		self.blob_hashes_to_check = collection_utils.deduplicated_list(blob_hashes_to_check)

	@override
	def run(self, *, session: Optional[DbSession] = None) -> BlobDeltaSummary:
		"""
		:param session: If provided, use this session for DB operations.
		NOTES: `session.commit()` will be called, so it's better to call this at the end of a `DbAccess.open_session()` block
		"""
		with contextlib.ExitStack() as es:
			if session is None:
				session = es.enter_context(DbAccess.open_session())

			orphan_blob_hashes = session.filtered_orphan_blob_hashes(self.blob_hashes_to_check)
			self.logger.debug('Found {}/{} orphan blobs to delete'.format(len(orphan_blob_hashes), len(self.blob_hashes_to_check)))

			if len(orphan_blob_hashes) > 0:
				action = DeleteBlobsAction(hashes=orphan_blob_hashes, raise_if_not_found=False)
				bld = action.run(session=session)
			else:
				bld = BlobDeltaSummary.zero()
				session.commit()

		return bld
