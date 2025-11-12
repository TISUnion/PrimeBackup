from typing import Optional

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.exceptions import BlobNotFound, BlobHashNotUnique
from prime_backup.types.blob_info import BlobInfo


class GetBlobAction(Action[BlobInfo]):
	def __init__(self, blob_hash: str, *, count_files: bool = False, sample_file_num: Optional[int] = None):
		super().__init__()
		self.blob_hash = blob_hash
		self.count_files = count_files
		self.sample_file_num = sample_file_num

	@override
	def run(self) -> BlobInfo:
		"""
		:raise: BlobNotFound
		"""
		with DbAccess.open_session() as session:
			blob = session.get_blob(self.blob_hash)
			file_count = session.get_file_count_by_blob_hashes([blob.hash]) if self.count_files else 0
			file_samples = session.get_file_by_blob_hashes([blob.hash], limit=self.sample_file_num) if self.sample_file_num is not None else None
			return BlobInfo.of(blob, file_count=file_count, file_samples=file_samples)


class GetBlobByHashPrefixAction(Action[BlobInfo]):
	def __init__(self, blob_hash_prefix: str, *, count_files: bool = False, sample_file_num: Optional[int] = None):
		super().__init__()
		self.blob_hash_prefix = blob_hash_prefix
		self.count_files = count_files
		self.sample_file_num = sample_file_num

	@override
	def run(self) -> BlobInfo:
		"""
		:raise: BlobNotFound or BlobHashNotUnique
		"""
		with DbAccess.open_session() as session:
			blobs = session.list_blob_with_hash_prefix(self.blob_hash_prefix, limit=3)
			if len(blobs) == 0:
				raise BlobNotFound(self.blob_hash_prefix)
			elif len(blobs) > 1:
				raise BlobHashNotUnique(self.blob_hash_prefix, list(sorted(map(BlobInfo.of, blobs))))

			blob = blobs[0]
			file_count = session.get_file_count_by_blob_hashes([blob.hash]) if self.count_files else 0
			file_samples = session.get_file_by_blob_hashes([blob.hash], limit=self.sample_file_num) if self.sample_file_num is not None else None
			return BlobInfo.of(blob, file_count=file_count, file_samples=file_samples)
