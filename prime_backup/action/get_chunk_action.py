from typing import List

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.types.chunk_info import OffsetChunkInfo, ChunkInfo


class GetChunkByIdAction(Action[ChunkInfo]):
	def __init__(self, chunk_id: int):
		super().__init__()
		self.chunk_id = chunk_id

	@override
	def run(self) -> ChunkInfo:
		"""
		:raise: ChunkIdNotFound
		"""
		with DbAccess.open_session() as session:
			chunk = session.get_chunk_by_id(self.chunk_id)
			return ChunkInfo.of(chunk)


class GetChunkByHashAction(Action[ChunkInfo]):
	def __init__(self, chunk_hash: str):
		super().__init__()
		self.chunk_hash = chunk_hash

	@override
	def run(self) -> ChunkInfo:
		"""
		:raise: ChunkHashNotFound
		"""
		with DbAccess.open_session() as session:
			chunk = session.get_chunk_by_hash(self.chunk_hash)
			return ChunkInfo.of(chunk)


class GetBlobChunksAction(Action[List[OffsetChunkInfo]]):
	def __init__(self, blob_id: int):
		super().__init__()
		self.blob_id = blob_id

	@override
	def run(self) -> List[OffsetChunkInfo]:
		with DbAccess.open_session() as session:
			oc_list = session.get_blob_chunks(self.blob_id)
			return [OffsetChunkInfo.of(oc) for oc in oc_list]
