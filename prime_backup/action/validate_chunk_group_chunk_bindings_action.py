import dataclasses
import enum
from typing import List, Dict

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.types.chunk_group_chunk_binding_info import ChunkGroupChunkBindingInfo


class BadChunkGroupChunkBindingItemType(enum.Enum):
	orphan = enum.auto()


@dataclasses.dataclass(frozen=True)
class BadChunkGroupChunkBindingItem:
	binding: ChunkGroupChunkBindingInfo
	typ: BadChunkGroupChunkBindingItemType
	desc: str


@dataclasses.dataclass
class ValidateChunkGroupsResult:
	total: int = 0
	bad_bindings: List[BadChunkGroupChunkBindingItem] = dataclasses.field(default_factory=list)

	@property
	def ok(self) -> int:
		return self.total - self.bad

	@property
	def bad(self) -> int:
		return len(self.bad_bindings)

	def add_bad(self, binding: ChunkGroupChunkBindingInfo, typ: BadChunkGroupChunkBindingItemType, msg: str):
		self.bad_bindings.append(BadChunkGroupChunkBindingItem(binding, typ, msg))

	def group_bad_by_type(self) -> Dict[BadChunkGroupChunkBindingItemType, List[BadChunkGroupChunkBindingItem]]:
		result: Dict[BadChunkGroupChunkBindingItemType, List[BadChunkGroupChunkBindingItem]] = {}
		for bad_chunk in self.bad_bindings:
			result.setdefault(bad_chunk.typ, []).append(bad_chunk)
		return result


class ValidateChunkGroupChunkBindingsAction(Action[ValidateChunkGroupsResult]):
	"""
	NOTE: BlobChunkGroupBinding's .chunk_offset and .chunk_id checks are done in ValidateChunkGroupsAction
	"""

	@override
	def run(self) -> ValidateChunkGroupsResult:
		self.logger.info('Scanning all chunk group chunk bindings for orphan check')
		result = ValidateChunkGroupsResult()

		session: DbSession
		with DbAccess.open_session() as session:
			result.total = session.get_chunk_group_chunk_binding_count()
			for binding in session.list_orphan_chunk_group_chunk_bindings(limit=1000):
				result.add_bad(ChunkGroupChunkBindingInfo.of(binding), BadChunkGroupChunkBindingItemType.orphan, f'orphan binding refers to a non-existent chunk group {binding.chunk_group_id}')

		self.logger.info('ChunkGroupChunkBinding validation done: total {}, ok {}, bad {}'.format(
			result.total, result.ok, result.bad,
		))
		return result
