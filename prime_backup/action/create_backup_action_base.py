import functools
from abc import ABC
from pathlib import Path
from typing import List, Callable, Optional

from prime_backup.action import Action
from prime_backup.db import schema
from prime_backup.db.session import DbSession
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.blob_info import BlobInfo, BlobListSummary


class CreateBackupActionBase(Action[BackupInfo], ABC):
	def __init__(self):
		super().__init__()
		self.__new_blobs: List[BlobInfo] = []
		self.__new_blobs_summary: Optional[BlobListSummary] = None
		self.__blobs_rollbackers: List[Callable] = []

	def _remove_file(self, file_to_remove: Path, *, what: str = 'rollback'):
		try:
			file_to_remove.unlink(missing_ok=True)
		except OSError as e:
			self.logger.error('({}) remove file {!r} failed: {}'.format(what, file_to_remove, e))

	def _add_remove_file_rollbacker(self, file_to_remove: Path):
		self.__blobs_rollbackers.append(functools.partial(self._remove_file, file_to_remove=file_to_remove))

	def _apply_blob_rollback(self):
		if len(self.__blobs_rollbackers) > 0:
			self.logger.warning('Error occurs during backup creation, applying rollback')
			for rollback_func in self.__blobs_rollbackers:
				rollback_func()
			self.__blobs_rollbackers.clear()

	def _create_blob(self, session: DbSession, **kwargs) -> schema.Blob:
		blob = session.create_blob(**kwargs)
		self.__new_blobs.append(BlobInfo.of(blob))
		return blob

	def get_new_blobs_summary(self) -> BlobListSummary:
		if self.__new_blobs_summary is None:
			self.__new_blobs_summary = BlobListSummary.of(self.__new_blobs)
		return self.__new_blobs_summary

	@classmethod
	def _finalize_backup_and_files(cls, session: DbSession, backup: schema.Backup, files: List[schema.File]):
		# flush to generate the backup id
		session.flush()

		file_raw_size_sum = 0
		file_stored_size_sum = 0

		for file in files:
			file.backup_id = backup.id
			if file.blob_raw_size is not None:
				file_raw_size_sum += file.blob_raw_size
			if file.blob_stored_size is not None:
				file_stored_size_sum += file.blob_stored_size
			session.add(file)

		backup.file_raw_size_sum = file_raw_size_sum
		backup.file_stored_size_sum = file_stored_size_sum

	def run(self) -> None:
		self.__new_blobs.clear()
		self.__new_blobs_summary = None
		self.__blobs_rollbackers.clear()
