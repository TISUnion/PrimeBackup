import contextlib
import logging
from pathlib import Path
from typing import List, TYPE_CHECKING

from typing_extensions import Unpack

from prime_backup.action.helpers import create_backup_utils
from prime_backup.db import schema
from prime_backup.db.session import DbSession
from prime_backup.types.blob_info import BlobInfo, BlobDeltaSummary
from prime_backup.types.chunk_info import ChunkInfo

if TYPE_CHECKING:
	from prime_backup.action.helpers.pack_writer import PackWriter


class BlobRecorder:
	def __init__(self, pack_writer: 'PackWriter'):
		from prime_backup import logger
		self.logger: logging.Logger = logger.get()
		self.__pack_writer = pack_writer

		self.__new_blobs: List[BlobInfo] = []
		self.__new_chunks: List[ChunkInfo] = []
		self.__file_rollbackers: List[Path] = []  # direct blob files to delete on rollback

	def add_remove_file_rollbacker(self, file_to_remove: Path):
		self.__file_rollbackers.append(file_to_remove)

	def apply_file_rollback(self):
		self.logger.warning('Error occurs during backup creation, applying rollback')

		# blobs and chunks
		if self.__file_rollbackers:
			for p in self.__file_rollbackers:
				create_backup_utils.remove_file(p, what='rollback')
			self.__file_rollbackers.clear()

		# pack files
		with contextlib.suppress(OSError):
			self.__pack_writer.close()  # close before delete
		if pack_paths := self.__pack_writer.get_rollback_paths():
			for p in pack_paths:
				create_backup_utils.remove_file(p, what='rollback')

	def create_blob(self, session: DbSession, **kwargs: Unpack[DbSession.CreateBlobKwargs]) -> schema.Blob:
		blob = session.create_and_add_blob(**kwargs)
		self.__new_blobs.append(BlobInfo.of(blob))
		return blob

	def record_new_chunk(self, new_chunk: ChunkInfo):
		self.__new_chunks.append(new_chunk)

	def get_blob_storage_delta(self) -> BlobDeltaSummary:
		return BlobDeltaSummary.of(self.__new_blobs, self.__new_chunks, packs=self.__pack_writer.get_created_pack_summary())
