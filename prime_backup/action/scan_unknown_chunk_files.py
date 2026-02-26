import dataclasses
import os
from pathlib import Path
from typing import List, Optional

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.utils import chunk_utils


@dataclasses.dataclass(frozen=True)
class UnknownChunkFile:
	path: Path
	chunk_hash: str  # from file name
	file_size: int


@dataclasses.dataclass(frozen=True)
class ScanUnknownChunkFilesResult:
	count: int
	size: int
	samples: List[UnknownChunkFile]


class ScanUnknownChunkFilesAction(Action[ScanUnknownChunkFilesResult]):
	def __init__(self, delete: bool, result_sample_limit: Optional[int] = 0):
		super().__init__()
		self.delete = delete
		self.result_sample_limit = result_sample_limit

	@override
	def run(self) -> ScanUnknownChunkFilesResult:
		count = 0
		size_sum = 0
		result_files: List[UnknownChunkFile] = []

		self.logger.info('Scanning chunk store to check if there are any unknown chunk files')
		unknown_chunk_file_samples: List[str] = []
		with DbAccess.open_session() as session:
			for chunk_dir in chunk_utils.iterate_chunk_directories():
				if not chunk_dir.is_dir():
					continue
				file_names = [
					name
					for name in os.listdir(chunk_dir)
					if (chunk_dir / name).is_file()
				]
				chunks = session.get_chunks_by_hashes_opt(file_names)
				for chunk_hash, chunk in chunks.items():
					if chunk is None:
						unknown_chunk_file: Path = chunk_dir / chunk_hash
						if not unknown_chunk_file.is_file():
							continue

						count += 1
						size = unknown_chunk_file.stat().st_size
						size_sum += size

						self.logger.debug('Found unknown chunk file at {} with size {}{}'.format(unknown_chunk_file, size, ', deleting' if self.delete else ''))
						if self.delete:
							unknown_chunk_file.unlink(missing_ok=True)

						if len(unknown_chunk_file_samples) < 5:
							unknown_chunk_file_samples.append(str(unknown_chunk_file))
						if self.result_sample_limit is not None and len(result_files) < self.result_sample_limit:
							result_files.append(UnknownChunkFile(unknown_chunk_file, chunk_hash, size))

		self.logger.info('Found and deleted {} unknown chunk files ({} bytes) in the chunk store, samples: {}'.format(count, size_sum, unknown_chunk_file_samples))

		return ScanUnknownChunkFilesResult(count, size_sum, result_files)
