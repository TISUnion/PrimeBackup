import dataclasses
import os

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.utils import blob_utils


@dataclasses.dataclass
class ScanAndDeleteUnknownBlobFilesResult:
	count: int
	size: int


class ScanAndDeleteUnknownBlobFilesAction(Action[ScanAndDeleteUnknownBlobFilesResult]):
	@override
	def run(self) -> ScanAndDeleteUnknownBlobFilesResult:
		count = 0
		size_sum = 0

		self.logger.info('Scanning blob store to check if there are any unknown blob files')
		with DbAccess.open_session() as session:
			for blob_dir in blob_utils.iterate_blob_directories():
				file_names = os.listdir(blob_dir)
				blobs = session.get_blobs(file_names)
				for blob_hash, blob in blobs.items():
					if blob is None:
						unknown_blob_file = blob_dir / blob_hash
						count += 1
						size_sum += unknown_blob_file.stat().st_size
						unknown_blob_file.unlink()
		self.logger.info('Found {} unknown blob files ({} bytes) in the blob store'.format(count, size_sum))

		return ScanAndDeleteUnknownBlobFilesResult(count, size_sum)
