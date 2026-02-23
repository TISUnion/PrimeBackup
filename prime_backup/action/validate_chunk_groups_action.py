import collections
import dataclasses
import enum
from typing import List, Dict

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.types.chunk_group_info import ChunkGroupInfo
from prime_backup.utils import chunk_utils


class BadChunkGroupItemType(enum.Enum):
	invalid = enum.auto()
	orphan = enum.auto()
	bad_layout = enum.auto()
	bad_chunk_stats = enum.auto()


@dataclasses.dataclass(frozen=True)
class BadChunkGroupItem:
	chunk_group: ChunkGroupInfo
	typ: BadChunkGroupItemType
	desc: str


@dataclasses.dataclass
class ValidateChunkGroupsResult:
	total: int = 0
	validated: int = 0
	bad_chunk_groups: List[BadChunkGroupItem] = dataclasses.field(default_factory=list)

	@property
	def ok(self) -> int:
		return self.validated - len(self.bad_chunk_groups)

	@property
	def bad(self) -> int:
		return len(self.bad_chunk_groups)

	def add_bad(self, chunk_group: ChunkGroupInfo, typ: BadChunkGroupItemType, msg: str):
		self.bad_chunk_groups.append(BadChunkGroupItem(chunk_group, typ, msg))

	def group_bad_by_type(self) -> Dict[BadChunkGroupItemType, List[BadChunkGroupItem]]:
		result: Dict[BadChunkGroupItemType, List[BadChunkGroupItem]] = {}
		for bad_chunk in self.bad_chunk_groups:
			result.setdefault(bad_chunk.typ, []).append(bad_chunk)
		return result


class ValidateChunkGroupsAction(Action[ValidateChunkGroupsResult]):
	"""
	Validate ChunkGroup + ChunkGroupChunkBinding
	"""

	@override
	def is_interruptable(self) -> bool:
		return True

	def __validate(self, session: DbSession, result: ValidateChunkGroupsResult, chunk_groups: List[ChunkGroupInfo]):
		all_chunk_group_ids = [cg.id for cg in chunk_groups]
		all_orphan_chunk_group_ids = set(session.filtered_orphan_chunk_group_ids(all_chunk_group_ids))
		all_bindings = session.get_chunk_group_chunk_bindings_for_chunk_groups(all_chunk_group_ids)
		all_bindings_by_chunk_group_id: Dict[int, List[schema.ChunkGroupChunkBinding]] = collections.defaultdict(list)
		for bd in all_bindings:
			all_bindings_by_chunk_group_id[bd.chunk_group_id].append(bd)
		for bd_lst in all_bindings_by_chunk_group_id.values():
			def group_binding_key_getter(b_: schema.ChunkGroupChunkBinding):
				return b_.chunk_offset

			bd_lst.sort(key=group_binding_key_getter)
		all_chunks_by_id = session.get_chunks_by_ids(list({binding.chunk_id for binding in all_bindings}))

		def validate_one_chunk_group(chunk_group: ChunkGroupInfo):
			if chunk_group.id <= 0:
				result.add_bad(chunk_group, BadChunkGroupItemType.invalid, f'chunk group with invalid id {chunk_group.id}')
				return

			if chunk_group.id in all_orphan_chunk_group_ids:
				result.add_bad(chunk_group, BadChunkGroupItemType.orphan, f'orphan chunk group with 0 associated blob binding')
				return

			group_bindings = all_bindings_by_chunk_group_id.get(chunk_group.id, [])
			if len(group_bindings) != chunk_group.chunk_count:
				result.add_bad(chunk_group, BadChunkGroupItemType.bad_chunk_stats, f'chunk count mismatch, expect {chunk_group.chunk_count}, found {len(group_bindings)} bindings')
				return

			# Validate chunk group layout
			chunk_hashes: List[str] = []
			raw_size_sum = 0
			stored_size_sum = 0
			offset = 0
			for binding in group_bindings:
				if offset != binding.chunk_offset:
					result.add_bad(chunk_group, BadChunkGroupItemType.bad_layout, f'chunk group binding offset mismatch, expect {offset}, actual {binding.chunk_offset}')
					return
				chunk = all_chunks_by_id.get(binding.chunk_id)
				if chunk is None:
					result.add_bad(chunk_group, BadChunkGroupItemType.bad_layout, f'chunk group binding at offset {offset} refers to a not-exists chunk {binding.chunk_id}')
					return

				chunk_hashes.append(chunk.hash)
				raw_size_sum += chunk.raw_size
				stored_size_sum += chunk.stored_size
				offset += chunk.raw_size

			expected_chunk_group_hash = chunk_utils.create_chunk_group_hash(chunk_hashes)
			if expected_chunk_group_hash != chunk_group.hash:
				result.add_bad(chunk_group, BadChunkGroupItemType.bad_layout, f'chunk group hash mismatch, expect {expected_chunk_group_hash}, found {chunk_group.hash}')
				return
			if raw_size_sum != chunk_group.chunk_raw_size_sum:
				result.add_bad(chunk_group, BadChunkGroupItemType.bad_chunk_stats, f'raw size sum mismatch, expect {raw_size_sum}, found {chunk_group.chunk_raw_size_sum}')
				return
			if stored_size_sum != chunk_group.chunk_stored_size_sum:
				result.add_bad(chunk_group, BadChunkGroupItemType.bad_chunk_stats, f'stored size sum mismatch, expect {stored_size_sum}, found {chunk_group.chunk_stored_size_sum}')
				return

		for cg in chunk_groups:
			if self.is_interrupted.is_set():
				break
			result.validated += 1
			validate_one_chunk_group(cg)

	@override
	def run(self) -> ValidateChunkGroupsResult:
		self.logger.info('Chunk group validation start')
		result = ValidateChunkGroupsResult()

		with DbAccess.open_session() as session:
			result.total = session.get_chunk_group_count()
			cnt = 0
			for chunk_groups in session.iterate_chunk_group_batch(batch_size=40):  # 40 ~= 5000 / chunk_constants.CHUNK_GROUP_AVG_SIZE
				if self.is_interrupted.is_set():
					break
				cnt += len(chunk_groups)
				self.logger.info('Validating {} / {} chunk groups'.format(cnt, result.total))
				self.__validate(session, result, list(map(ChunkGroupInfo.of, chunk_groups)))

		self.logger.info('Chunk group validation done: total {}, validated {}, ok {}, bad {}'.format(
			result.total, result.validated, result.ok, result.bad,
		))
		return result
