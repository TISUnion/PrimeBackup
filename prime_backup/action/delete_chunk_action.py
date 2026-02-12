import contextlib
import logging
from typing import List, Dict, Optional, Collection

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.exceptions import ChunkIdNotFound, ChunkHashNotFound
from prime_backup.types.chunk_info import ChunkInfo, ChunkListSummary
from prime_backup.utils import collection_utils


class _ChunkTrashBin:
	def __init__(self, logger: logging.Logger):
		self.trash_chunks: List[ChunkInfo] = []
		self.logger = logger
		self.errors: List[Exception] = []

	def add(self, chunk_info: ChunkInfo):
		self.trash_chunks.append(chunk_info)

	def make_summary(self) -> ChunkListSummary:
		return ChunkListSummary.of(self.trash_chunks)

	def erase_all(self):
		for trash in self.trash_chunks:
			try:
				trash.chunk_file_path.unlink(missing_ok=True)
			except Exception as e:
				self.logger.error('Error erasing chunk {} at {!r}'.format(trash.hash, trash.chunk_file_path))
				self.errors.append(e)


class DeleteChunksAction(Action[ChunkListSummary]):
	def __init__(self, *, ids: Collection[int] = (), hashes: Collection[str] = (), raise_if_not_found: bool = True):
		super().__init__()
		self.chunk_ids = collection_utils.deduplicated_list(ids)
		self.chunk_hashes = collection_utils.deduplicated_list(hashes)
		self.raise_if_not_found = raise_if_not_found

	@override
	def run(self, *, session: Optional[DbSession] = None) -> ChunkListSummary:
		"""
		:param session: If provided, use this session for DB operations.
		NOTES: `session.commit()` will be called, so it's better to call this at the end of a `DbAccess.open_session()` block
		"""
		trash_bin = _ChunkTrashBin(self.logger)

		with contextlib.ExitStack() as es:
			if session is None:
				session = es.enter_context(DbAccess.open_session())

			all_to_delete_chunks = self.__collect_chunks_to_delete(session)
			for chunk in all_to_delete_chunks.values():
				trash_bin.add(chunk)

			session.delete_chunks_by_ids([chunk.id for chunk in all_to_delete_chunks.values()])
			session.commit()

		s = trash_bin.make_summary()
		trash_bin.erase_all()

		if len(errors := trash_bin.errors) > 0:
			self.logger.error('Found {} chunk erasing failure in total'.format(len(errors)))
			raise errors[0]

		return s

	def __collect_chunks_to_delete(self, session: DbSession) -> Dict[int, ChunkInfo]:
		self_chunk_ids_set = set(self.chunk_ids)
		self_chunk_hashes_set = set(self.chunk_hashes)
		all_to_delete_chunks: Dict[int, ChunkInfo] = {}

		chunks_by_id: Dict[int, schema.Chunk] = session.get_chunks_by_ids(self.chunk_ids)
		for chunk_id, chunk in chunks_by_id.items():
			if chunk is None and self.raise_if_not_found:
				raise ChunkIdNotFound(chunk_id)
			if chunk_id not in self_chunk_ids_set:
				raise AssertionError('got unexpected chunk id {!r}, should be in {}'.format(chunk_id, self_chunk_ids_set))
			all_to_delete_chunks[chunk.id] = ChunkInfo.of(chunk)

		chunks_by_hash: Dict[str, schema.Chunk] = session.get_chunks_by_hashes(self.chunk_hashes)
		for chunk_hash, chunk in chunks_by_hash.items():
			if chunk is None and self.raise_if_not_found:
				raise ChunkHashNotFound(chunk_hash)
			if chunk_hash not in self_chunk_hashes_set:
				raise AssertionError('got unexpected chunk hash {!r}, should be in {}'.format(chunk_hash, self_chunk_hashes_set))
			all_to_delete_chunks[chunk.id] = ChunkInfo.of(chunk)

		return all_to_delete_chunks


class DeleteOrphanChunksAction(Action[ChunkListSummary]):
	def __init__(self, *, ids: Collection[int]):
		super().__init__()
		self.chunk_ids_to_check = collection_utils.deduplicated_list(ids)

	@override
	def run(self, *, session: Optional[DbSession] = None) -> ChunkListSummary:
		"""
		:param session: If provided, use this session for DB operations.
		NOTES: `session.commit()` will be called, so it's better to call this at the end of a `DbAccess.open_session()` block
		"""
		with contextlib.ExitStack() as es:
			if session is None:
				session = es.enter_context(DbAccess.open_session())

			orphan_chunk_ids = session.filtered_orphan_chunk_ids(self.chunk_ids_to_check)

			if len(orphan_chunk_ids) > 0:
				action = DeleteChunksAction(
					ids=orphan_chunk_ids,
					raise_if_not_found=True,
				)
				bls = action.run(session=session)
			else:
				bls = ChunkListSummary.zero()
				session.commit()

		return bls
