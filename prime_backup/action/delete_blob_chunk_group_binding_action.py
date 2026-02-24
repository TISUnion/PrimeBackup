import contextlib
from typing import List, Optional

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.action.delete_chunk_group_action import DeleteOrphanChunkGroupsAction
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.db.values import BlobChunkGroupBindingIdentifier
from prime_backup.utils import collection_utils


class DeleteBlobChunkGroupBindingsAction(Action[None]):
	def __init__(self, identifiers: List[BlobChunkGroupBindingIdentifier]):
		super().__init__()
		self.identifiers = identifiers

	@override
	def run(self, *, session: Optional[DbSession] = None) -> None:
		with contextlib.ExitStack() as es:
			if session is None:
				session = es.enter_context(DbAccess.open_session())

			bindings = session.get_blob_chunk_group_bindings(self.identifiers)
			chunk_group_ids = collection_utils.deduplicated_list(binding.chunk_group_id for binding in bindings.values())
			session.delete_blob_chunk_group_bindings(self.identifiers)

			orphan_chunk_group_cnt = DeleteOrphanChunkGroupsAction(ids=chunk_group_ids).run(session=session)

		self.logger.debug('Deleted {} BlobChunkGroupBindings and {} orphan chunk groups'.format(len(bindings), orphan_chunk_group_cnt))
