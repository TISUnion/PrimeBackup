import contextlib
from typing import List, Optional

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.action.delete_chunk_action import DeleteOrphanChunksAction
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.db.values import ChunkGroupChunkBindingIdentifier
from prime_backup.types.units import ByteCount
from prime_backup.utils import collection_utils


class DeleteChunkGroupChunkBindingsAction(Action[None]):
	def __init__(self, identifiers: List[ChunkGroupChunkBindingIdentifier]):
		super().__init__()
		self.identifiers = identifiers

	@override
	def run(self, *, session: Optional[DbSession] = None) -> None:
		"""
		:param session: If provided, use this session for DB operations.
		NOTES: `session.commit()` will be called, so it's better to call this at the end of a `DbAccess.open_session()` block
		"""
		session: DbSession
		with contextlib.ExitStack() as es:
			if session is None:
				session = es.enter_context(DbAccess.open_session())

			bindings = session.get_chunk_group_chunk_bindings(self.identifiers)
			chunk_ids = collection_utils.deduplicated_list(binding.chunk_id for binding in bindings.values())
			session.delete_chunk_group_chunk_bindings(self.identifiers)

			summary = DeleteOrphanChunksAction(ids=chunk_ids).run(session=session)

		self.logger.debug('Deleted {} ChunkGroupChunkBindings and {} orphan chunks (size {} / {})'.format(
			len(bindings), summary.count, ByteCount(summary.stored_size).auto_str(), ByteCount(summary.raw_size).auto_str(),
		))
