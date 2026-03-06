from abc import ABC, abstractmethod
from typing import Optional

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.exceptions import BlobHashNotFound, BlobHashNotUnique
from prime_backup.types.blob_info import BlobInfo


class _GetBlobActionBase(Action[BlobInfo], ABC):
	def __init__(self, count_files: bool = False, sample_file_num: Optional[int] = None):
		super().__init__()
		self.count_files = count_files
		self.sample_file_num = sample_file_num

	@override
	def run(self) -> BlobInfo:
		"""
		:raise: BlobHashNotFound
		"""
		with DbAccess.open_session() as session:
			blob = self._do_get_blob(session)
			file_count = session.get_file_count_by_blob_hashes([blob.hash]) if self.count_files else 0
			file_samples = session.get_file_by_blob_hashes([blob.hash], limit=self.sample_file_num) if self.sample_file_num is not None else None
			return BlobInfo.of(blob, file_count=file_count, file_samples=file_samples)

	@abstractmethod
	def _do_get_blob(self, session: DbSession) -> schema.Blob:
		...


class GetBlobByIdAction(_GetBlobActionBase):
	def __init__(self, blob_id: int, *, count_files: bool = False, sample_file_num: Optional[int] = None):
		super().__init__(count_files, sample_file_num)
		self.blob_id = blob_id

	@override
	def _do_get_blob(self, session: DbSession) -> schema.Blob:
		return session.get_blob_by_id(self.blob_id)


class GetBlobByHashAction(_GetBlobActionBase):
	def __init__(self, blob_hash: str, *, count_files: bool = False, sample_file_num: Optional[int] = None):
		super().__init__(count_files, sample_file_num)
		self.blob_hash = blob_hash

	@override
	def _do_get_blob(self, session: DbSession) -> schema.Blob:
		return session.get_blob_by_hash(self.blob_hash)


class GetBlobByHashPrefixAction(_GetBlobActionBase):
	def __init__(self, blob_hash_prefix: str, *, count_files: bool = False, sample_file_num: Optional[int] = None):
		super().__init__(count_files, sample_file_num)
		self.blob_hash_prefix = blob_hash_prefix

	@override
	def run(self) -> BlobInfo:
		"""
		:raise: BlobHashNotFound or BlobHashNotUnique
		"""
		return super().run()

	@override
	def _do_get_blob(self, session: DbSession) -> schema.Blob:
		blobs = session.list_blob_with_hash_prefix(self.blob_hash_prefix, limit=3)
		if len(blobs) == 0:
			raise BlobHashNotFound(self.blob_hash_prefix)
		elif len(blobs) > 1:
			def get_hash_for_sort(b: 'BlobInfo'):
				return b.hash
			raise BlobHashNotUnique(self.blob_hash_prefix, sorted(map(BlobInfo.of, blobs), key=get_hash_for_sort))
		return blobs[0]
