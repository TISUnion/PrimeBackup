import dataclasses
import os
from pathlib import Path
from typing import List, Optional

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.utils import blob_utils


@dataclasses.dataclass(frozen=True)
class UnknownBlobFile:
	path: Path
	blob_hash: str  # from file name
	file_size: int


@dataclasses.dataclass(frozen=True)
class ScanUnknownBlobFilesResult:
	count: int
	size: int
	samples: List[UnknownBlobFile]


class ScanUnknownBlobFilesAction(Action[ScanUnknownBlobFilesResult]):
	def __init__(self, delete: bool, result_sample_limit: Optional[int] = 0):
		super().__init__()
		self.delete = delete
		self.result_sample_limit = result_sample_limit

	@override
	def run(self) -> ScanUnknownBlobFilesResult:
		count = 0
		size_sum = 0
		result_files: List[UnknownBlobFile] = []

		self.logger.info('Scanning blob store to check if there are any unknown blob files')
		unknown_blob_file_samples: List[str] = []
		with DbAccess.open_session() as session:
			for blob_dir in blob_utils.iterate_blob_directories():
				if not blob_dir.is_dir():
					continue
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

						if self.delete:
							unknown_blob_file.unlink(missing_ok=True)
						if len(unknown_blob_file_samples) < 5:
							unknown_blob_file_samples.append(str(unknown_blob_file))
						if self.result_sample_limit is not None and len(result_files) < self.result_sample_limit:
							result_files.append(UnknownBlobFile(unknown_blob_file, blob_hash, size))

		self.logger.info('Found and deleted {} unknown blob files ({} bytes) in the blob store, samples: {}'.format(count, size_sum, unknown_blob_file_samples))

		return ScanUnknownBlobFilesResult(count, size_sum, result_files)
