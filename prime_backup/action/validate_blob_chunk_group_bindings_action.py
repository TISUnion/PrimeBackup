import contextlib
import dataclasses
import enum
from typing import List, Optional

from typing_extensions import override, Dict

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.types.blob_chunk_group_binding_info import BlobChunkGroupBindingInfo


class BadBlobChunkGroupBindingItemType(enum.Enum):
	orphan = enum.auto()
	bad_storage_method = enum.auto()


@dataclasses.dataclass(frozen=True)
class BadBlobChunkGroupBindingItem:
	binding: BlobChunkGroupBindingInfo
	typ: BadBlobChunkGroupBindingItemType
	desc: str


@dataclasses.dataclass
class ValidateBlobChunkGroupBindingsResult:
	total: int = 0
	bad_bindings: List[BadBlobChunkGroupBindingItem] = dataclasses.field(default_factory=list)

	@property
	def ok(self) -> int:
		return self.total - self.bad

	@property
	def bad(self) -> int:
		return len(self.bad_bindings)

	def add_bad(self, binding: BlobChunkGroupBindingInfo, typ: BadBlobChunkGroupBindingItemType, msg: str):
		self.bad_bindings.append(BadBlobChunkGroupBindingItem(binding, typ, msg))

	def group_bad_by_type(self) -> Dict[BadBlobChunkGroupBindingItemType, List[BadBlobChunkGroupBindingItem]]:
		result: Dict[BadBlobChunkGroupBindingItemType, List[BadBlobChunkGroupBindingItem]] = {}
		for bad_chunk in self.bad_bindings:
			result.setdefault(bad_chunk.typ, []).append(bad_chunk)
		return result


class ValidateBlobChunkGroupBindingsAction(Action[ValidateBlobChunkGroupBindingsResult]):
	"""
	NOTE: BlobChunkGroupBinding's .chunk_group_offset and .chunk_group_id checks are done in ValidateBlobsAction
	"""

	@override
	def run(self, *, session: Optional[DbSession] = None) -> ValidateBlobChunkGroupBindingsResult:
		self.logger.info('Scanning all blob chunk group bindings for orphan check')
		result = ValidateBlobChunkGroupBindingsResult()

		session: DbSession
		with contextlib.ExitStack() as es:
			if session is None:
				session = es.enter_context(DbAccess.open_session())
			result.total = session.get_blob_chunk_group_binding_count()

			for lor in session.list_orphan_blob_chunk_group_bindings(limit=1000):
				if lor.blob is None:
					result.add_bad(BlobChunkGroupBindingInfo.of(lor.binding), BadBlobChunkGroupBindingItemType.orphan, f'orphan binding refers to a non-existent blob {lor.binding.blob_id}')
				else:
					result.add_bad(BlobChunkGroupBindingInfo.of(lor.binding), BadBlobChunkGroupBindingItemType.bad_storage_method, f'Chunk group binding has invalid storage method {lor.blob.storage_method}')

		self.logger.info('BlobChunkGroupBindingInfo validation done: total {}, ok {}, bad {}'.format(
			result.total, result.ok, result.bad,
		))
		return result
