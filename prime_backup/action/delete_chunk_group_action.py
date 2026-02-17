import contextlib
from typing import Dict, Optional, Collection, List

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.action.delete_chunk_action import DeleteOrphanChunksAction
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.exceptions import ChunkIdNotFound, ChunkHashNotFound
from prime_backup.utils import collection_utils


class DeleteChunkGroupsAction(Action[None]):
	def __init__(self, *, ids: Collection[int] = (), hashes: Collection[str] = (), raise_if_not_found: bool = True):
		super().__init__()
		self.chunk_group_ids = collection_utils.deduplicated_list(ids)
		self.chunk_group_hashes = collection_utils.deduplicated_list(hashes)
		self.raise_if_not_found = raise_if_not_found

	@override
	def run(self, *, session: Optional[DbSession] = None) -> None:
		"""
		:param session: If provided, use this session for DB operations.
		NOTES: `session.commit()` will be called, so it's better to call this at the end of a `DbAccess.open_session()` block
		"""
		with contextlib.ExitStack() as es:
			if session is None:
				session = es.enter_context(DbAccess.open_session())

			all_to_delete_chunk_group_ids = self.__collect_chunk_group_ids_to_delete(session)
			if len(all_to_delete_chunk_group_ids) > 0:
				# 1. delete ChunkGroup-Chunk bindings
				# 2. delete chunk groups
				# 3. check orphan chunks
				bindings = session.get_chunk_group_chunk_bindings_for_chunk_groups(all_to_delete_chunk_group_ids)
				affected_chunk_ids = {cgc.chunk_id: None for cgc in bindings}  # ordered set
				session.delete_chunk_group_chunk_bindings_for_chunk_groups(all_to_delete_chunk_group_ids)

				session.delete_chunk_groups_by_ids(all_to_delete_chunk_group_ids)
				DeleteOrphanChunksAction(ids=affected_chunk_ids.keys()).run(session=session)
			else:
				session.commit()
		self.logger.debug('Deleted {} chunk groups'.format(len(all_to_delete_chunk_group_ids)))

	def __collect_chunk_group_ids_to_delete(self, session: DbSession) -> List[int]:
		self_chunk_group_ids_set = set(self.chunk_group_ids)
		self_chunk_group_hashes_set = set(self.chunk_group_hashes)
		all_to_delete_chunk_groups: Dict[int, None] = {}  # ordered set

		chunk_groups_by_id: Dict[int, schema.ChunkGroup] = session.get_chunk_groups_by_ids(self.chunk_group_ids)
		for chunk_group_id, chunk_group in chunk_groups_by_id.items():
			if chunk_group is None and self.raise_if_not_found:
				raise ChunkIdNotFound(chunk_group_id)
			if chunk_group_id not in self_chunk_group_ids_set:
				raise AssertionError('got unexpected chunk group id {!r}, should be in {}'.format(chunk_group_id, self_chunk_group_ids_set))
			all_to_delete_chunk_groups[chunk_group.id] = None

		chunk_groups_by_hash: Dict[str, schema.ChunkGroup] = session.get_chunk_groups_by_hashes(self.chunk_group_hashes)
		for chunk_group_hash, chunk_group in chunk_groups_by_hash.items():
			if chunk_group is None and self.raise_if_not_found:
				raise ChunkHashNotFound(chunk_group_hash)
			if chunk_group_hash not in self_chunk_group_hashes_set:
				raise AssertionError('got unexpected chunk group hash {!r}, should be in {}'.format(chunk_group_hash, self_chunk_group_hashes_set))
			all_to_delete_chunk_groups[chunk_group.id] = None

		return list(all_to_delete_chunk_groups.keys())


class DeleteOrphanChunkGroupsAction(Action[None]):
	def __init__(self, *, ids: Collection[int]):
		super().__init__()
		self.chunk_ids_to_check = collection_utils.deduplicated_list(ids)

	@override
	def run(self, *, session: Optional[DbSession] = None) -> None:
		"""
		:param session: If provided, use this session for DB operations.
		NOTES: `session.commit()` will be called, so it's better to call this at the end of a `DbAccess.open_session()` block
		"""
		with contextlib.ExitStack() as es:
			if session is None:
				session = es.enter_context(DbAccess.open_session())

			orphan_chunk_group_ids = session.filtered_orphan_chunk_group_ids(self.chunk_ids_to_check)
			self.logger.debug('Found {}/{} orphan chunk groups to delete'.format(len(orphan_chunk_group_ids), len(self.chunk_ids_to_check)))

			if len(orphan_chunk_group_ids) > 0:
				DeleteChunkGroupsAction(ids=orphan_chunk_group_ids, raise_if_not_found=True).run(session=session)
			else:
				session.commit()
