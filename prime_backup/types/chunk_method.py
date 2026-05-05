import enum
from pathlib import Path
from typing import Optional, IO, TYPE_CHECKING

from prime_backup.types.chunker import Chunker
from prime_backup.types.chunker_definition import ChunkerDefinition, FastCDCChunkerDefinition, FixedSizeChunkerDefinition
from prime_backup.utils.path_like import PathLike


class ChunkMethod(enum.Enum):
	# Content-Defined Chunking with FastCDC
	fastcdc_32k = FastCDCChunkerDefinition(avg_size=32 * 1024, min_size=8 * 1024, max_size=256 * 1024)
	fastcdc_128k = FastCDCChunkerDefinition(avg_size=128 * 1024, min_size=64 * 1024, max_size=1024 * 1024)

	# Fixed-Size Chunking
	fixed_4k = FixedSizeChunkerDefinition(4 * 1024)
	fixed_32k = FixedSizeChunkerDefinition(32 * 1024)
	fixed_128k = FixedSizeChunkerDefinition(128 * 1024)

	# Common Alias
	cdc = fastcdc_32k

	if TYPE_CHECKING:
		value: ChunkerDefinition

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
