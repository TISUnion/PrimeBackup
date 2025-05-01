import dataclasses
import os
from pathlib import Path
from typing import List

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
		unknown_blob_file_samples: List[str] = []
		with DbAccess.open_session() as session:
			for blob_dir in blob_utils.iterate_blob_directories():
				file_names = [
					name
					for name in os.listdir(blob_dir)
					if (blob_dir / name).is_file()
				]
				blobs = session.get_blobs(file_names)
				for blob_hash, blob in blobs.items():
					if blob is None:
						unknown_blob_file: Path = blob_dir / blob_hash
						if not unknown_blob_file.is_file():
							continue

						count += 1
						size = unknown_blob_file.stat().st_size
						size_sum += size
						self.logger.debug('Found unknown blob at {} with size {}, deleting'.format(unknown_blob_file, size))
						unknown_blob_file.unlink(missing_ok=True)
						if len(unknown_blob_file_samples) < 5:
							unknown_blob_file_samples.append(str(unknown_blob_file))

		self.logger.info('Found and deleted {} unknown blob files ({} bytes) in the blob store, samples: {}'.format(count, size_sum, unknown_blob_file_samples))

		return ScanAndDeleteUnknownBlobFilesResult(count, size_sum)
