import logging
from typing import List, Dict

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.exceptions import BlobNotFound
from prime_backup.types.blob_info import BlobInfo, BlobListSummary


class _BlobTrashBin:
	def __init__(self, logger: logging.Logger):
		self.trash_blobs: List[BlobInfo] = []
		self.logger = logger
		self.errors: List[Exception] = []

	def add(self, blob_info: BlobInfo):
		self.trash_blobs.append(blob_info)

	def make_summary(self) -> BlobListSummary:
		return BlobListSummary.of(self.trash_blobs)

	def erase_all(self):
		for trash in self.trash_blobs:
			try:
				trash.blob_path.unlink(missing_ok=True)
			except Exception as e:
				self.logger.error('Error erasing blob {} at {!r}'.format(trash.hash, trash.blob_path))
				self.errors.append(e)


class DeleteBlobsAction(Action[BlobListSummary]):
	def __init__(self, blob_hashes: List[str], *, raise_if_not_found: bool = True):
		super().__init__()
		self.blob_hashes = blob_hashes
		self.raise_if_not_found = raise_if_not_found

	@override
	def run(self) -> BlobListSummary:
		trash_bin = _BlobTrashBin(self.logger)
		self_blob_hashes_set = set(self.blob_hashes)

		with DbAccess.open_session() as session:
			blobs: Dict[str, schema.Blob] = session.get_blobs(self.blob_hashes)
			collected_hashes: List[str] = []
			for blob_hash, blob in blobs.items():
				if blob is None and self.raise_if_not_found:
					raise BlobNotFound(blob_hash)
				else:
					if blob_hash not in self_blob_hashes_set:
						raise AssertionError('got unexpected blob hash {!r}, should be in {}'.format(blob_hash, self_blob_hashes_set))
					collected_hashes.append(blob_hash)
					trash_bin.add(BlobInfo.of(blob))

			session.delete_blobs(self.blob_hashes)

		s = trash_bin.make_summary()
		trash_bin.erase_all()

		if len(errors := trash_bin.errors) > 0:
			self.logger.error('Found {} blob erasing failure in total'.format(len(errors)))
			raise errors[0]

		return s
