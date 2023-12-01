import functools
from abc import ABC
from pathlib import Path
from typing import List, Callable, NamedTuple

from prime_backup.action import Action
from prime_backup.db import schema
from prime_backup.db.session import DbSession
from prime_backup.types.blob_info import BlobInfo


class CreateBackupActionBase(Action, ABC):
	def __init__(self):
		super().__init__()
		self.__new_blobs: List[BlobInfo] = []
		self.__blobs_rollbackers: List[Callable] = []

	def _remove_file(self, file_to_remove: Path):
		try:
			if file_to_remove.is_file():
				file_to_remove.unlink(missing_ok=True)
		except OSError as e:
			self.logger.error('(rollback) remove file {!r} failed: {}'.format(file_to_remove, e))

	def _add_remove_file_rollbacker(self, file_to_remove: Path):
		self.__blobs_rollbackers.append(functools.partial(self._remove_file, file_to_remove=file_to_remove))

	def _apply_blob_rollback(self):
		if len(self.__blobs_rollbackers) > 0:
			self.__blobs_rollbackers.clear()
			self.logger.info('Error occurs during import, applying rollback')
			for rollback_func in self.__blobs_rollbackers:
				rollback_func()

	def _create_blob(self, session: DbSession, **kwargs) -> schema.Blob:
		blob = session.create_blob(**kwargs)
		self.__new_blobs.append(BlobInfo.of(blob))
		return blob

	class NewBlobSummary(NamedTuple):
		count: int
		stored_size: int
		raw_size: int

	def _summarize_new_blobs(self) -> NewBlobSummary:
		return self.NewBlobSummary(
			len(self.__new_blobs),
			sum([b.stored_size for b in self.__new_blobs]),
			sum([b.raw_size for b in self.__new_blobs]),
		)

	def run(self) -> None:
		self.__new_blobs.clear()
		self.__blobs_rollbackers.clear()