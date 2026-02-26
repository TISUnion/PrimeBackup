import dataclasses
import logging
import time
from typing import Dict, List, Optional

from prime_backup.action.helpers.create_backup_utils import CreateBackupTimeCostKey
from prime_backup.constants import chunk_constants
from prime_backup.db import schema
from prime_backup.db.session import DbSession
from prime_backup.utils import chunk_utils, collection_utils
from prime_backup.utils.time_cost_stats import TimeCostStats


@dataclasses.dataclass
class _RawChunkGroup:
	offset: int = -1  # offset in blob
	hash: str = ''
	chunks: List[schema.Chunk] = dataclasses.field(default_factory=list)


_dummy_chunk_costs: TimeCostStats[CreateBackupTimeCostKey] = TimeCostStats()


class ChunkGrouper:
	def __init__(self, session: DbSession, time_costs: Optional[TimeCostStats[CreateBackupTimeCostKey]]):
		from prime_backup import logger
		from prime_backup.config.config import Config
		self.logger: logging.Logger = logger.get()
		self.config: Config = Config.get()

		self.session = session
		self.__time_costs: TimeCostStats[CreateBackupTimeCostKey] = time_costs or _dummy_chunk_costs

	def create_chunk_groups(self, blob: schema.Blob, blob_chunks: Dict[int, schema.Chunk]):
		"""
		blob and chunks should have their .id generated
		:param blob: the blob that is split to chunks
		:param blob_chunks: offset -> chunk
		"""

		start_time = time.time()

		# cut chunks to chunk groups

		raw_chunk_groups: List[_RawChunkGroup] = []
		chunk_group_hashes_to_chunks: Dict[str, List[schema.Chunk]] = {}
		current_group = _RawChunkGroup()
		for i, chunk_pair in enumerate(blob_chunks.items()):
			offset, chunk = chunk_pair

			current_group.chunks.append(chunk)
			if current_group.offset < 0:
				current_group.offset = offset

			assert chunk_constants.CHUNK_GROUP_AVG_SIZE == 128
			needs_cut = False
			needs_cut |= i == len(blob_chunks) - 1  # last chunk group
			if len(current_group.chunks) >= chunk_constants.CHUNK_GROUP_MIN_SIZE:
				needs_cut |= len(current_group.chunks) >= chunk_constants.CHUNK_GROUP_MAX_SIZE  # reach max size
				needs_cut |= chunk.hash.endswith('00') or chunk.hash.endswith('80')  # 1/128 chance
			if needs_cut:
				current_group.hash = chunk_utils.create_chunk_group_hash(chunk.hash for chunk in current_group.chunks)
				raw_chunk_groups.append(current_group)
				chunk_group_hashes_to_chunks[current_group.hash] = current_group.chunks
				current_group = _RawChunkGroup()

		# create new chunk groups
		with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_db):
			known_chunk_groups = collection_utils.no_none_dict(self.session.get_chunk_groups_by_hashes([rcg.hash for rcg in raw_chunk_groups]))
		new_chunk_group_hashes: List[str] = []
		for cg_hash, cg_chunks in chunk_group_hashes_to_chunks.items():
			if cg_hash not in known_chunk_groups:
				new_chunk_group = self.session.create_and_add_chunk_group(
					hash=cg_hash,
					chunk_count=len(cg_chunks),
					chunk_raw_size_sum=sum(chunk.raw_size for chunk in cg_chunks),
					chunk_stored_size_sum=sum(chunk.stored_size for chunk in cg_chunks),
				)
				known_chunk_groups[cg_hash] = new_chunk_group
				new_chunk_group_hashes.append(new_chunk_group.hash)
		if len(new_chunk_group_hashes) > 0:
			with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_db):
				self.session.flush()  # creates chunk_group.id

		# create bindings for new chunk groups
		for cg_hash in new_chunk_group_hashes:
			new_chunk_group = known_chunk_groups[cg_hash]
			offset = 0
			for chunk in chunk_group_hashes_to_chunks[cg_hash]:
				self.session.create_and_add_chunk_group_chunk_binding(
					chunk_group_id=new_chunk_group.id,
					chunk_offset=offset,
					chunk_id=chunk.id,
				)
				offset += chunk.raw_size

		# create binding for the blob
		for raw_chunk_group in raw_chunk_groups:
			db_chunk_group = known_chunk_groups[raw_chunk_group.hash]
			self.session.create_and_add_blob_chunk_group_binding(
				blob_id=blob.id,
				chunk_group_offset=raw_chunk_group.offset,
				chunk_group_id=db_chunk_group.id,
			)

		# end
		cost_sec = time.time() - start_time
		self.logger.debug('blob chunks finalized in {:.2f}s, chunk group cnt {} (+{})'.format(
			cost_sec, len(raw_chunk_groups), len(new_chunk_group_hashes),
		))
