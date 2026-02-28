import logging
from pathlib import Path
from typing import List, Callable, Optional, Any

from typing_extensions import Unpack

from prime_backup.action.helpers import create_backup_utils
from prime_backup.db import schema
from prime_backup.db.session import DbSession
from prime_backup.types.blob_info import BlobInfo, BlobDeltaSummary
from prime_backup.types.chunk_info import ChunkInfo


class BlobRecorder:
	def __init__(self):
		from prime_backup import logger
		self.logger: logging.Logger = logger.get()

		self.__new_blobs: List[BlobInfo] = []
		self.__new_chunks: List[ChunkInfo] = []
		self.__blob_storage_delta_cache: Optional[BlobDeltaSummary] = None
		self.__file_rollbackers: List[Callable[[], Any]] = []

	def add_remove_file_rollbacker(self, file_to_remove: Path):
		def func():
			create_backup_utils.remove_file(file_to_remove, what='rollback')

		self.__file_rollbackers.append(func)

	def apply_file_rollback(self):
		if len(self.__file_rollbackers) > 0:
			self.logger.warning('Error occurs during backup creation, applying rollback')
			for rollback_func in self.__file_rollbackers:
				rollback_func()
			self.__file_rollbackers.clear()

	def create_blob(self, session: DbSession, **kwargs: Unpack[DbSession.CreateBlobKwargs]) -> schema.Blob:
		blob = session.create_and_add_blob(**kwargs)
		self.__new_blobs.append(BlobInfo.of(blob))
		return blob

	def record_chunk(self, chunk: schema.Chunk):
		self.__new_chunks.append(ChunkInfo.of(chunk))

	def get_blob_storage_delta(self) -> BlobDeltaSummary:
		if self.__blob_storage_delta_cache is None:
			self.__blob_storage_delta_cache = BlobDeltaSummary.of(self.__new_blobs, self.__new_chunks)
		return self.__blob_storage_delta_cache
