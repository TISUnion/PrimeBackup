import dataclasses
import logging
import time
from typing import Dict, List, Optional

from prime_backup.action.helpers.create_backup_utils import CreateBackupTimeCostKey
from prime_backup.constants import chunk_constants
from prime_backup.db import schema
from prime_backup.db.session import DbSession
from prime_backup.utils import chunk_utils
from prime_backup.utils.time_cost_stats import TimeCostStats

_dummy_chunk_costs: TimeCostStats[CreateBackupTimeCostKey] = TimeCostStats()


class ChunkGrouper:
	@dataclasses.dataclass(frozen=True)
	class ChunkLike:
		id: int
		hash: str
		raw_size: int
		stored_size: int

		@classmethod
		def of(cls, chunk: schema.Chunk) -> 'ChunkGrouper.ChunkLike':
			return cls(
				id=chunk.id,
				hash=chunk.hash,
				raw_size=chunk.raw_size,
				stored_size=chunk.stored_size,
			)

	@dataclasses.dataclass
	class _RawChunkGroup:
		offset: int = -1  # offset in blob
		hash: str = ''
		chunks: List['ChunkGrouper.ChunkLike'] = dataclasses.field(default_factory=list)

	def __init__(self, session: DbSession, time_costs: Optional[TimeCostStats[CreateBackupTimeCostKey]]):
		from prime_backup import logger
		from prime_backup.config.config import Config
		self.logger: logging.Logger = logger.get()
		self.config: Config = Config.get()

		self.session = session
		self.__time_costs: TimeCostStats[CreateBackupTimeCostKey] = time_costs or _dummy_chunk_costs

	def create_chunk_groups(self, blob: schema.Blob, blob_chunks: Dict[int, ChunkLike]):
		"""
		blob and chunks should have their .id generated
		:param blob: the blob that is split to chunks
		:param blob_chunks: offset -> chunk
		"""

		start_time = time.time()

		# cut chunks to chunk groups

		raw_chunk_groups: List[ChunkGrouper._RawChunkGroup] = []
		chunk_group_hashes_to_chunks: Dict[str, List[ChunkGrouper.ChunkLike]] = {}
		current_group = self._RawChunkGroup()
		for i, chunk_pair in enumerate(blob_chunks.items()):
			offset, chunk = chunk_pair

			current_group.chunks.append(chunk)
			if current_group.offset < 0:
				current_group.offset = offset

			assert chunk_constants.CHUNK_GROUP_AVG_SIZE == 256
			needs_cut = False
			needs_cut |= i == len(blob_chunks) - 1  # last chunk group
			if len(current_group.chunks) >= chunk_constants.CHUNK_GROUP_MIN_SIZE:
				needs_cut |= len(current_group.chunks) >= chunk_constants.CHUNK_GROUP_MAX_SIZE  # reach max size
				needs_cut |= chunk.hash.endswith('00')  # 1/256 chance
			if needs_cut:
				current_group.hash = chunk_utils.create_chunk_group_hash(chunk.hash for chunk in current_group.chunks)
				raw_chunk_groups.append(current_group)
				chunk_group_hashes_to_chunks[current_group.hash] = current_group.chunks
				current_group = self._RawChunkGroup()

		# create new chunk groups
		with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_db):
			known_chunk_group_hash_to_id = self.session.get_chunk_group_ids_by_hashes_opt([rcg.hash for rcg in raw_chunk_groups])
		new_chunk_groups: List[schema.ChunkGroup] = []
		new_chunk_group_hashes: List[str] = []
		for cg_hash, cg_chunks in chunk_group_hashes_to_chunks.items():
			if known_chunk_group_hash_to_id[cg_hash] is None:
				new_chunk_group = self.session.create_and_add_chunk_group(
					hash=cg_hash,
					chunk_count=len(cg_chunks),
					chunk_raw_size_sum=sum(chunk.raw_size for chunk in cg_chunks),
					chunk_stored_size_sum=sum({chunk.hash: chunk.stored_size for chunk in cg_chunks}.values()),
				)
				new_chunk_groups.append(new_chunk_group)
				new_chunk_group_hashes.append(new_chunk_group.hash)
		if len(new_chunk_group_hashes) > 0:
			with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_db):
				self.session.flush()  # creates chunk_group.id
			known_chunk_group_hash_to_id.update({
				new_chunk_group.hash: new_chunk_group.id
				for new_chunk_group in new_chunk_groups
			})
		for cg_hash, cg_id in known_chunk_group_hash_to_id.items():
			if cg_id is None:
				raise AssertionError('chunk group id for hash {} still not exists'.format(cg_hash))

		# create bindings for new chunk groups
		chunk_group_chunk_binding_rows: List[DbSession.CreateChunkGroupChunkBindingKwargs] = []
		for cg_hash in new_chunk_group_hashes:
			chunk_group_id = known_chunk_group_hash_to_id[cg_hash]
			assert chunk_group_id is not None
			offset = 0
			for chunk in chunk_group_hashes_to_chunks[cg_hash]:
				chunk_group_chunk_binding_rows.append({
					'chunk_group_id': chunk_group_id,
					'chunk_offset': offset,
					'chunk_id': chunk.id,
				})
				offset += chunk.raw_size
		self.session.insert_chunk_group_chunk_bindings(chunk_group_chunk_binding_rows)

		# create binding for the blob
		for raw_chunk_group in raw_chunk_groups:
			chunk_group_id = known_chunk_group_hash_to_id[raw_chunk_group.hash]
			assert chunk_group_id is not None
			self.session.create_and_add_blob_chunk_group_binding(
				blob_id=blob.id,
				chunk_group_offset=raw_chunk_group.offset,
				chunk_group_id=chunk_group_id,
			)

		# end
		if self.logger.isEnabledFor(logging.DEBUG):
			cost_sec = time.time() - start_time
			self.logger.debug('blob chunks finalized in {:.2f}s, chunk group count {} (+{})'.format(
				cost_sec, len(raw_chunk_groups), len(new_chunk_group_hashes),
			))
