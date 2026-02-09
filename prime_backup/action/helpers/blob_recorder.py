import logging
from pathlib import Path
from typing import List, Callable, Optional, Any

from typing_extensions import Unpack

from prime_backup.action.helpers import create_backup_utils
from prime_backup.db import schema
from prime_backup.db.session import DbSession
from prime_backup.types.blob_info import BlobInfo, BlobListSummary


class BlobRecorder:
	def __init__(self):
		from prime_backup import logger
		self.logger: logging.Logger = logger.get()

		self.__new_blobs: List[BlobInfo] = []
		self.__new_blobs_summary: Optional[BlobListSummary] = None
		self.__blobs_rollbackers: List[Callable[[], Any]] = []

	def add_remove_file_rollbacker(self, file_to_remove: Path):
		def func():
			create_backup_utils.remove_file(file_to_remove, what='rollback')

		self.__blobs_rollbackers.append(func)

	def apply_blob_rollback(self):
		if len(self.__blobs_rollbackers) > 0:
			self.logger.warning('Error occurs during backup creation, applying rollback')
			for rollback_func in self.__blobs_rollbackers:
				rollback_func()
			self.__blobs_rollbackers.clear()

	def create_blob(self, session: DbSession, **kwargs: Unpack[DbSession.CreateBlobKwargs]) -> schema.Blob:
		blob = session.create_and_add_blob(**kwargs)
		self.__new_blobs.append(BlobInfo.of(blob))
		return blob

	def get_new_blobs_summary(self) -> BlobListSummary:
		if self.__new_blobs_summary is None:
			self.__new_blobs_summary = BlobListSummary.of(self.__new_blobs)
		return self.__new_blobs_summary
