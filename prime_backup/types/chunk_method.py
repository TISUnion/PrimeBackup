import enum
from pathlib import Path
from typing import Optional, IO

from prime_backup.types.chunker_factory import CDCChunkerFactory, Fixed4KChunkerFactory
from prime_backup.utils.chunker import Chunker
from prime_backup.utils.path_like import PathLike


class ChunkMethod(enum.Enum):
	cdc_32k = CDCChunkerFactory()
	fixed_4k = Fixed4KChunkerFactory()

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
		if not backup_config.chunking_enabled:
			return None

		for cfg in backup_config.chunking_rules:
			if file_size >= cfg.file_size_threshold and cfg.patterns_spec.match_file(file_path):
				return cfg.algorithm

		return None

	def create_file_chunker(self, file_path: Path, need_entire_file_hash: bool) -> Chunker:
		return self.value.create_file_chunker(file_path, need_entire_file_hash)

	def create_stream_chunker(self, stream: IO[bytes], need_entire_file_hash: bool) -> Chunker:
		return self.value.create_stream_chunker(stream, need_entire_file_hash)
