from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.exceptions import ChunkGroupHashNotUnique, ChunkGroupHashNotFound
from prime_backup.types.chunk_group_info import ChunkGroupInfo


class GetChunkGroupByIdAction(Action[ChunkGroupInfo]):
	def __init__(self, chunk_group_id: int):
		super().__init__()
		self.chunk_group_id = chunk_group_id

	@override
	def run(self) -> ChunkGroupInfo:
		"""
		:raise: ChunkGroupIdNotFound
		"""
		with DbAccess.open_session() as session:
			chunk_group = session.get_chunk_group_by_id(self.chunk_group_id)
			return ChunkGroupInfo.of(chunk_group)


class GetChunkGroupByHashAction(Action[ChunkGroupInfo]):
	def __init__(self, chunk_group_hash: str):
		super().__init__()
		self.chunk_group_hash = chunk_group_hash

	@override
	def run(self) -> ChunkGroupInfo:
		"""
		:raise: ChunkGroupHashNotFound
		"""
		with DbAccess.open_session() as session:
			chunk_group = session.get_chunk_group_by_hash(self.chunk_group_hash)
			return ChunkGroupInfo.of(chunk_group)


class GetChunkGroupByHashPrefixAction(Action[ChunkGroupInfo]):
	def __init__(self, chunk_group_hash_prefix: str):
		super().__init__()
		self.chunk_group_hash_prefix = chunk_group_hash_prefix

	@override
	def run(self) -> ChunkGroupInfo:
		"""
		:raise: ChunkGroupHashNotFound or ChunkGroupHashNotUnique
		"""
		with DbAccess.open_session() as session:
			chunk_groups = session.list_chunk_group_with_hash_prefix(self.chunk_group_hash_prefix, limit=3)
			if len(chunk_groups) == 0:
				raise ChunkGroupHashNotFound(self.chunk_group_hash_prefix)
			elif len(chunk_groups) > 1:
				def get_hash_for_sort(b: 'ChunkGroupInfo'):
					return b.hash
				raise ChunkGroupHashNotUnique(self.chunk_group_hash_prefix, sorted(map(ChunkGroupInfo.of, chunk_groups), key=get_hash_for_sort))
			return ChunkGroupInfo.of(chunk_groups[0])
