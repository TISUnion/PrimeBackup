import dataclasses
from abc import abstractmethod, ABC
from typing import List, Set, TypeVar, Generic, Iterable

from typing_extensions import override, Protocol

from prime_backup.action import Action
from prime_backup.action.delete_blob_action import DeleteBlobsAction, DeleteOrphanBlobsAction
from prime_backup.action.delete_blob_chunk_group_binding_action import DeleteBlobChunkGroupBindingsAction
from prime_backup.action.delete_chunk_action import DeleteChunksAction
from prime_backup.action.delete_chunk_group_action import DeleteChunkGroupsAction
from prime_backup.action.delete_chunk_group_chunk_binding_action import DeleteChunkGroupChunkBindingsAction
from prime_backup.action.delete_file_action import DeleteFilesStep
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.db.values import BlobChunkGroupBindingIdentifier, ChunkGroupChunkBindingIdentifier
from prime_backup.types.blob_info import BlobListSummary
from prime_backup.types.chunk_info import ChunkListSummary
from prime_backup.types.file_info import FileListSummary
from prime_backup.types.fileset_info import FilesetListSummary


@dataclasses.dataclass
class _SimpleSummary:
	count: int = 0


class __ResultWithCount(Protocol):
	count: int


_T = TypeVar('_T')  # object type
_K = TypeVar('_K')  # object key
_R = TypeVar('_R', bound=__ResultWithCount)  # result


class _ScanAndDeleteObjectsActionBase(Generic[_T, _K, _R], Action[_R], ABC):
	MAX_IN_MEMORY_OBJECTS = 10000

	def __init__(self, what: str, show_progress_step: int):
		super().__init__()
		self.what = what
		self.show_progress_step = show_progress_step

	@override
	def run(self) -> _R:
		result = self._create_result()

		while True:
			limit_reached = False

			session: DbSession
			with DbAccess.open_session() as session:
				total_count = self._get_total_count(session)
				checking_count = 0
				orphan_obj_keys: List[_K] = []
				for objs in self._batch_iterate(session):
					checking_count += len(objs)
					if checking_count % self.show_progress_step == 0 or checking_count == total_count:
						self.logger.info('Checking {} / {} {}'.format(checking_count, total_count, self.what))
					orphan_obj_keys.extend(self._filter_orphans(session, objs))
					if len(orphan_obj_keys) > self.MAX_IN_MEMORY_OBJECTS:
						limit_reached = True
						break
				if len(orphan_obj_keys) > 0:
					self.logger.info('Found {} orphaned {}s, deleting'.format(len(orphan_obj_keys), self.what))
					self._delete_orphans(session, result, orphan_obj_keys)

			if not limit_reached:
				break

			self.logger.info('limit_reached, next round')

		if result.count > 0:
			self.logger.info('Found and deleted {} orphaned {}s in total'.format(result.count, self.what))
		else:
			self.logger.info('Found 0 orphaned {} in total'.format(self.what))
		return result

	@abstractmethod
	def _create_result(self) -> _R:
		pass

	@abstractmethod
	def _get_total_count(self, session: DbSession) -> int:
		pass

	@abstractmethod
	def _batch_iterate(self, session: DbSession) -> Iterable[List[_T]]:
		pass

	@abstractmethod
	def _filter_orphans(self, session: DbSession, objs: List[_T]) -> List[_K]:
		pass

	@abstractmethod
	def _delete_orphans(self, session: DbSession, result: _R, orphan_obj_keys: List[_K]) -> None:
		pass


class ScanAndDeleteOrphanBlobsAction(_ScanAndDeleteObjectsActionBase[schema.Blob, str, BlobListSummary]):
	def __init__(self):
		super().__init__('blobs', 3000)

	@override
	def _create_result(self) -> BlobListSummary:
		return BlobListSummary.zero()

	@override
	def _get_total_count(self, session: DbSession) -> int:
		return session.get_blob_count()

	@override
	def _batch_iterate(self, session: DbSession) -> Iterable[List[schema.Blob]]:
		return session.iterate_blob_batch(batch_size=500)

	@override
	def _filter_orphans(self, session: DbSession, objs: List[schema.Blob]) -> List[str]:
		return session.filtered_orphan_blob_hashes([blob.hash for blob in objs])

	@override
	def _delete_orphans(self, session: DbSession, result: BlobListSummary, orphan_obj_keys: List[str]) -> None:
		action = DeleteBlobsAction(hashes=orphan_obj_keys, raise_if_not_found=True)
		bls = action.run(session=session)
		result += bls


class ScanAndDeleteOrphanChunkGroupsAction(_ScanAndDeleteObjectsActionBase[schema.ChunkGroup, int, _SimpleSummary]):
	def __init__(self):
		super().__init__('chunk group', 3000)

	@override
	def _create_result(self) -> _SimpleSummary:
		return _SimpleSummary()

	@override
	def _get_total_count(self, session: DbSession) -> int:
		return session.get_chunk_group_count()

	@override
	def _batch_iterate(self, session: DbSession) -> Iterable[List[schema.ChunkGroup]]:
		return session.iterate_chunk_group_batch(batch_size=500)

	@override
	def _filter_orphans(self, session: DbSession, objs: List[schema.ChunkGroup]) -> List[int]:
		return session.filtered_orphan_chunk_group_ids([chunk_group.id for chunk_group in objs])

	@override
	def _delete_orphans(self, session: DbSession, result: _SimpleSummary, orphan_obj_keys: List[int]) -> None:
		DeleteChunkGroupsAction(ids=orphan_obj_keys, raise_if_not_found=True).run(session=session)
		result.count += len(orphan_obj_keys)


class ScanAndDeleteOrphanChunksAction(_ScanAndDeleteObjectsActionBase[schema.Chunk, int, ChunkListSummary]):
	def __init__(self):
		super().__init__('chunk', 3000)

	@override
	def _create_result(self) -> ChunkListSummary:
		return ChunkListSummary.zero()

	@override
	def _get_total_count(self, session: DbSession) -> int:
		return session.get_chunk_count()

	@override
	def _batch_iterate(self, session: DbSession) -> Iterable[List[schema.Chunk]]:
		return session.iterate_chunk_batch(batch_size=500)

	@override
	def _filter_orphans(self, session: DbSession, objs: List[schema.Chunk]) -> List[int]:
		return session.filtered_orphan_chunk_ids([chunk.id for chunk in objs])

	@override
	def _delete_orphans(self, session: DbSession, result: ChunkListSummary, orphan_obj_keys: List[int]) -> None:
		action = DeleteChunksAction(ids=orphan_obj_keys, raise_if_not_found=True)
		result += action.run(session=session)


class ScanAndDeleteOrphanChunkGroupChunkBindingsAction(Action[ChunkListSummary]):
	MAX_LOOP = 1000

	@override
	def run(self) -> _SimpleSummary:
		result = _SimpleSummary()
		session: DbSession

		with DbAccess.open_session() as session:
			total_cnt = session.get_chunk_group_chunk_binding_count()
		self.logger.info('Checking {} chunk group chunk bindings'.format(total_cnt))

		with DbAccess.open_session() as session:
			for _ in range(self.MAX_LOOP):
				bindings = [
					ChunkGroupChunkBindingIdentifier.of(binding)
					for binding in session.list_orphan_chunk_group_chunk_bindings(limit=1000)
				]
				if len(bindings) == 0:
					break
				result.count += len(bindings)
				DeleteChunkGroupChunkBindingsAction(bindings).run(session=session)
				session.flush()
		if result.count > 0:
			self.logger.info('Found and deleted {} orphaned chunk group chunk bindings in total'.format(result.count))
		else:
			self.logger.info('Found 0 orphaned chunk group chunk binding in total')
		return result


class ScanAndDeleteOrphanBlobChunkGroupBindingsAction(Action[ _SimpleSummary]):
	MAX_LOOP = 1000

	@override
	def run(self) -> _SimpleSummary:
		result = _SimpleSummary()
		session: DbSession

		with DbAccess.open_session() as session:
			total_cnt = session.get_blob_chunk_group_binding_count()
		self.logger.info('Checking {} blob chunk group bindings'.format(total_cnt))

		with DbAccess.open_session() as session:
			for _ in range(self.MAX_LOOP):
				bindings = [
					BlobChunkGroupBindingIdentifier.of(binding_item.binding)
					for binding_item in session.list_orphan_blob_chunk_group_bindings(limit=1000)
				]
				if len(bindings) == 0:
					break
				result.count += len(bindings)
				DeleteBlobChunkGroupBindingsAction(bindings).run(session=session)
				session.flush()
		if result.count > 0:
			self.logger.info('Found and deleted {} orphaned blob chunk group bindings in total'.format(result.count))
		else:
			self.logger.info('Found 0 orphaned blob chunk group binding in total')
		return result


class ScanAndDeleteOrphanFilesAction(Action[FileListSummary]):
	MAX_IN_MEMORY_OBJECTS = 10000

	def run(self) -> FileListSummary:
		fls = FileListSummary.zero()

		while True:
			limit_reached = False

			session: DbSession
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
				filesets = session.list_filesets()
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

					fsls.count += 1

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
	orphan_chunk_group_count: int
	orphan_chunk_count: int
	orphan_chunk_group_chunk_binding_count: int
	orphan_blob_chunk_group_binding_count: int
	orphan_file_count: int
	orphan_fileset_count: int

	@property
	def total_orphan_count(self) -> int:
		total = 0
		total += self.orphan_blob_count
		total += self.orphan_chunk_group_count
		total += self.orphan_chunk_count
		total += self.orphan_chunk_group_chunk_binding_count
		total += self.orphan_blob_chunk_group_binding_count
		total += self.orphan_file_count
		total += self.orphan_fileset_count
		return total


class ScanAndDeleteOrphanObjectsAction(Action[ScanAndDeleteOrphanObjectsResult]):
	def run(self) -> ScanAndDeleteOrphanObjectsResult:
		self.logger.info('Scanning orphan objects')
		orphan_blob_summary = ScanAndDeleteOrphanBlobsAction().run()
		orphan_chunk_group_summary = ScanAndDeleteOrphanChunkGroupsAction().run()
		orphan_chunk_summary = ScanAndDeleteOrphanChunksAction().run()
		orphan_chunk_group_chunk_binding_summary = ScanAndDeleteOrphanChunkGroupChunkBindingsAction().run()
		orphan_blob_chunk_group_binding_summary = ScanAndDeleteOrphanBlobChunkGroupBindingsAction().run()
		orphan_file_summary = ScanAndDeleteOrphanFilesAction().run()
		orphan_fileset_summary = ScanAndDeleteOrphanFilesetsAction().run()
		result = ScanAndDeleteOrphanObjectsResult(
			orphan_blob_count=orphan_blob_summary.count,
			orphan_chunk_group_count=orphan_chunk_group_summary.count,
			orphan_chunk_count=orphan_chunk_summary.count,
			orphan_chunk_group_chunk_binding_count=orphan_chunk_group_chunk_binding_summary.count,
			orphan_blob_chunk_group_binding_count=orphan_blob_chunk_group_binding_summary.count,
			orphan_file_count=orphan_file_summary.count,
			orphan_fileset_count=orphan_fileset_summary.count,
		)

		if result.total_orphan_count > 0:
			self.logger.info(
				'Found and deleted {} orphan objects in total: {} orphan blobs, '
				'{} orphan chunks, {} orphan chunk groups, '
				'{} orphan chunk group chunk bindings, {} orphan blob chunk group bindings, '
				'{} orphan file objects (with {} blobs), {} orphan fileset objects (with {} files and {} blobs)'.format(
					result.total_orphan_count, orphan_blob_summary.count,
					orphan_chunk_summary.count, orphan_chunk_group_summary.count,
					orphan_chunk_group_chunk_binding_summary.count, orphan_blob_chunk_group_binding_summary.count,
					orphan_file_summary.count, orphan_file_summary.blob_summary.count,
					orphan_fileset_summary.count, orphan_fileset_summary.file_summary.count, orphan_fileset_summary.file_summary.blob_summary.count,
				)
			)
		else:
			self.logger.info('Found 0 orphan object, everything looks good')
		return result
