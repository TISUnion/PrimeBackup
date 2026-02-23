import dataclasses
from typing import List

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.action.validate_blob_chunk_group_bindings_action import ValidateBlobChunkGroupBindingsResult, ValidateBlobChunkGroupBindingsAction
from prime_backup.action.validate_chunk_group_chunk_bindings_action import ValidateChunkGroupChunkBindingsResult, ValidateChunkGroupChunkBindingsAction
from prime_backup.action.validate_chunk_groups_action import ValidateChunkGroupsResult, ValidateChunkGroupsAction
from prime_backup.action.validate_chunks_action import ValidateChunksResult, ValidateChunksAction
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.types.blob_info import BlobInfo
from prime_backup.types.file_info import FileInfo
from prime_backup.utils import collection_utils


@dataclasses.dataclass
class ValidateChunkObjectsResult:
	chunk_result: ValidateChunksResult
	chunk_group_result: ValidateChunkGroupsResult
	chunk_group_chunk_bindings_result: ValidateChunkGroupChunkBindingsResult
	blob_chunk_group_bindings_result: ValidateBlobChunkGroupBindingsResult

	affected_blob_count: int = 0
	affected_blob_samples: List[BlobInfo] = dataclasses.field(default_factory=list)
	affected_file_count: int = 0
	affected_file_samples: List[FileInfo] = dataclasses.field(default_factory=list)
	affected_fileset_ids: List[int] = dataclasses.field(default_factory=list)
	affected_backup_ids: List[int] = dataclasses.field(default_factory=list)


class ValidateChunkObjectsAction(Action[ValidateChunkObjectsResult]):
	@override
	def is_interruptable(self) -> bool:
		return True

	@override
	def run(self) -> ValidateChunkObjectsResult:
		session: DbSession
		with DbAccess.open_session() as session:
			if not self.is_interrupted.is_set():
				chunk_result = ValidateChunksAction().run(session=session)
			if not self.is_interrupted.is_set():
				chunk_group_result = ValidateChunkGroupsAction().run(session=session)
			if not self.is_interrupted.is_set():
				chunk_group_chunk_bindings_result = ValidateChunkGroupChunkBindingsAction().run(session=session)
			if not self.is_interrupted.is_set():
				blob_chunk_group_bindings_result = ValidateBlobChunkGroupBindingsAction().run(session=session)

			result = ValidateChunkObjectsResult(
				chunk_result=chunk_result,
				chunk_group_result=chunk_group_result,
				chunk_group_chunk_bindings_result=chunk_group_chunk_bindings_result,
				blob_chunk_group_bindings_result=blob_chunk_group_bindings_result,
			)

			bad_chunk_ids = [bad_item.chunk.id for bad_item in chunk_result.bad_chunks]
			bad_chunk_group_ids = [bad_item.chunk_group.id for bad_item in chunk_group_result.bad_chunk_groups]
			if len(bad_chunk_ids) > 0:
				affected_chunk_group_ids = collection_utils.deduplicated_list(bad_chunk_group_ids + session.get_chunk_group_ids_by_chunk_ids(bad_chunk_ids))
			else:
				affected_chunk_group_ids = bad_chunk_group_ids

			if len(affected_chunk_group_ids) > 0:
				affected_blobs = session.get_blobs_by_chunk_group_ids(bad_chunk_group_ids)
				affected_blob_hashes = [blob.hash for blob in affected_blobs]
				result.affected_blob_count = len(affected_blobs)
				result.affected_blob_samples = [BlobInfo.of(blob) for blob in affected_blobs[:1000]]
				result.affected_file_count = session.get_file_count_by_blob_hashes(affected_blob_hashes)
				result.affected_file_samples = [FileInfo.of(file) for file in session.get_file_by_blob_hashes(affected_blob_hashes, limit=1000)]
				result.affected_fileset_ids = session.get_fileset_ids_by_blob_hashes(affected_blob_hashes)
				result.affected_backup_ids = session.get_backup_ids_by_fileset_ids(result.affected_fileset_ids)
			else:
				affected_blobs = []

			if len(bad_chunk_ids) > 0 or len(bad_chunk_group_ids) > 0:
				self.logger.debug('Found {} bad chunks and {} bad chunk groups, total affected chunk groups: {}, affected blobs: {}'.format(
					len(bad_chunk_ids), len(bad_chunk_group_ids), len(affected_chunk_group_ids), len(affected_blobs),
				))

		return result
