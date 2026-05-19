import contextlib
import logging
from typing import List, Dict, Optional, Collection

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.action.compact_packs_action import CollectPacksForCompactStep, CompactPacksAction
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.exceptions import ChunkIdNotFound, ChunkHashNotFound
from prime_backup.types.chunk_info import ChunkInfo, ChunkListSummary
from prime_backup.types.pack_info import PackChangeSummary
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
		pass


class DeleteChunksAction(Action[ChunkListSummary]):
	def __init__(self, *, ids: Collection[int] = (), hashes: Collection[str] = (), raise_if_not_found: bool = True):
		super().__init__()
		self.chunk_ids = collection_utils.deduplicated_list(ids)
		self.chunk_hashes = collection_utils.deduplicated_list(hashes)
		self.raise_if_not_found = raise_if_not_found
		self.pack_change_summary = PackChangeSummary.zero()

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

			affected_pack_ids = collection_utils.deduplicated_list(chunk.pack_entry.pack_id for chunk in all_to_delete_chunks.values() if chunk.pack_entry.pack_id > 0)
			pack_live_size_decrement: Dict[int, int] = {}
			pack_live_count_decrement: Dict[int, int] = {}
			for chunk in all_to_delete_chunks.values():
				if chunk.pack_entry.pack_id > 0:
					pack_live_size_decrement[chunk.pack_entry.pack_id] = pack_live_size_decrement.get(chunk.pack_entry.pack_id, 0) + chunk.stored_size
					pack_live_count_decrement[chunk.pack_entry.pack_id] = pack_live_count_decrement.get(chunk.pack_entry.pack_id, 0) + 1
			session.delete_chunks_by_ids([chunk.id for chunk in all_to_delete_chunks.values()])
			for pack_id, pack in session.get_packs_by_ids(affected_pack_ids).items():
				if pack is None:
					continue
				pack.live_size -= pack_live_size_decrement.get(pack_id, 0)
				pack.live_count -= pack_live_count_decrement.get(pack_id, 0)

			pack_ids_to_compact = CollectPacksForCompactStep(
				session,
				pack_ids=affected_pack_ids,
				threshold=self.config.backup.pack_compact_threshold,
				raise_if_not_found=False,
			).run().pack_ids
			pack_ids_to_compact_set = set(pack_ids_to_compact)
			updated_only_pack_ids = [pack_id for pack_id in affected_pack_ids if pack_id not in pack_ids_to_compact_set]
			if len(pack_ids_to_compact) > 0:
				self.pack_change_summary = CompactPacksAction(pack_ids_to_compact).run(session=session)
			else:
				session.commit()
			self.pack_change_summary.updated_pack_count += len(updated_only_pack_ids)

		s = trash_bin.make_summary()
		trash_bin.erase_all()
		self.logger.debug('Deleted {} chunks: {}'.format(len(all_to_delete_chunks), s))
		if self.pack_change_summary.changed_pack_count > 0:
			self.logger.debug('Changed packs after chunk deletion: {}'.format(self.pack_change_summary))

		if len(errors := trash_bin.errors) > 0:
			self.logger.error('Found {} chunk erasing failures in total'.format(len(errors)))
			raise errors[0]

		s.packs += self.pack_change_summary
		return s

	def __collect_chunks_to_delete(self, session: DbSession) -> Dict[int, ChunkInfo]:
		self_chunk_ids_set = set(self.chunk_ids)
		self_chunk_hashes_set = set(self.chunk_hashes)
		all_to_delete_chunks: Dict[int, ChunkInfo] = {}

		chunks_by_id: Dict[int, Optional[schema.Chunk]] = session.get_chunks_by_ids_opt(self.chunk_ids)
		for chunk_id, chunk in chunks_by_id.items():
			if chunk is None:
				if self.raise_if_not_found:
					raise ChunkIdNotFound(chunk_id)
				else:
					self.logger.warning('Chunk with id {} does not exist'.format(chunk_id))
					continue
			if chunk_id not in self_chunk_ids_set:
				raise AssertionError('got unexpected chunk id {!r}, should be in {}'.format(chunk_id, self_chunk_ids_set))
			all_to_delete_chunks[chunk.id] = ChunkInfo.of(chunk)

		chunks_by_hash: Dict[str, Optional[schema.Chunk]] = session.get_chunks_by_hashes_opt(self.chunk_hashes)
		for chunk_hash, chunk in chunks_by_hash.items():
			if chunk is None:
				if self.raise_if_not_found:
					raise ChunkHashNotFound(chunk_hash)
				else:
					self.logger.warning('Chunk with hash {} does not exist'.format(chunk_hash))
					continue
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
			self.logger.debug('Found {}/{} orphan chunks to delete'.format(len(orphan_chunk_ids), len(self.chunk_ids_to_check)))

			if len(orphan_chunk_ids) > 0:
				action = DeleteChunksAction(ids=orphan_chunk_ids, raise_if_not_found=False)
				summary = action.run(session=session)
			else:
				summary = ChunkListSummary.zero()
				session.commit()

		return summary
