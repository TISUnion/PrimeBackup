import enum
from typing import Optional

from prime_backup.utils.path_like import PathLike


class ChunkMethod(enum.Enum):
	cdc = enum.auto()
	fixed_4k = enum.auto()

	@classmethod
	def get_for_file(cls, file_path: PathLike, file_size: int) -> Optional['ChunkMethod']:
		"""
		Determine which chunking method to use for the given file.
		Returns None if the file should not be chunked
		"""
		from prime_backup.config.config import Config
		backup_config = Config.get().backup

		if file_size <= 0:
			return None

		if backup_config.f4k_enabled and file_size >= backup_config.f4k_file_size_threshold:
			if backup_config.f4k_patterns_spec.match_file(file_path):
				return ChunkMethod.fixed_4k

		if backup_config.cdc_enabled and file_size >= backup_config.cdc_file_size_threshold:
			if backup_config.cdc_patterns_spec.match_file(file_path):
				return ChunkMethod.cdc

		return None
