import dataclasses
import os
from pathlib import Path
from typing import List, Optional

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess


@dataclasses.dataclass(frozen=True)
class UnknownPackFile:
	path: Path
	pack_name: str
	file_size: int


@dataclasses.dataclass(frozen=True)
class ScanUnknownPackFilesResult:
	count: int
	size: int
	samples: List[UnknownPackFile]


class ScanUnknownPackFilesAction(Action[ScanUnknownPackFilesResult]):
	def __init__(self, delete: bool, result_sample_limit: Optional[int] = 0):
		super().__init__()
		self.delete = delete
		self.result_sample_limit = result_sample_limit

	@override
	def run(self) -> ScanUnknownPackFilesResult:
		from prime_backup.utils import pack_utils

		count = 0
		size_sum = 0
		result_files: List[UnknownPackFile] = []

		self.logger.info('Scanning pack store to check if there are any unknown pack files')
		unknown_pack_file_samples: List[str] = []
		with DbAccess.open_session() as session:
			db_pack_names = set(session.get_all_pack_names())
			for pack_dir in pack_utils.iterate_pack_directories():
				if not pack_dir.is_dir():
					continue
				for name in os.listdir(pack_dir):
					pack_file = pack_dir / name
					if not pack_file.is_file() or name in db_pack_names:
						continue

					count += 1
					size = pack_file.stat().st_size
					size_sum += size

					self.logger.debug('Found unknown pack file at {} with size {}{}'.format(pack_file, size, ', deleting' if self.delete else ''))
					if self.delete:
						pack_file.unlink(missing_ok=True)

					if len(unknown_pack_file_samples) < 5:
						unknown_pack_file_samples.append(str(pack_file))
					if self.result_sample_limit is not None and len(result_files) < self.result_sample_limit:
						result_files.append(UnknownPackFile(pack_file, name, size))

		self.logger.info('Found {} unknown pack files ({} bytes) in the pack store{}; samples: {}'.format(
			count, size_sum, ', deleted' if self.delete else '', unknown_pack_file_samples,
		))
		return ScanUnknownPackFilesResult(count, size_sum, result_files)
