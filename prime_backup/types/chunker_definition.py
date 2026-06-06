import dataclasses
from abc import abstractmethod, ABC
from pathlib import Path
from typing import IO, Optional, Iterable

from typing_extensions import override

from prime_backup.types.chunker import Chunker, FastCDCFileChunker, FastCDCStreamChunker, FixedSizeFileChunker, FixedSizeStreamChunker, FastCDCChunkerConfig, FixedAutoFileChunker, PrettyChunk, _get_fastcdc_class


class ChunkerDefinition(ABC):
	def ensure_lib(self):
		pass

	@abstractmethod
	def create_file_chunker(self, file_path: Path, need_entire_file_hash: bool, *, previous_chunks: Optional[Iterable[PrettyChunk]] = None) -> Chunker:
		...

	@abstractmethod
	def create_stream_chunker(self, stream, need_entire_file_hash: bool) -> Chunker:
		...

	def needs_previous_chunks(self) -> bool:
		return False


@dataclasses.dataclass(frozen=True)
class FastCDCChunkerDefinition(ChunkerDefinition):
	avg_size: int
	min_size: int
	max_size: int
	_config: FastCDCChunkerConfig = dataclasses.field(init=False, repr=False, compare=False)

	def __post_init__(self):
		object.__setattr__(self, '_config', FastCDCChunkerConfig(self.avg_size, self.min_size, self.max_size))

	@override
	def ensure_lib(self):
		_get_fastcdc_class()

	@override
	def create_file_chunker(self, file_path: Path, need_entire_file_hash: bool, *, previous_chunks: Optional[Iterable[PrettyChunk]] = None) -> Chunker:
		return FastCDCFileChunker(self._config, file_path, need_entire_file_hash)

	@override
	def create_stream_chunker(self, stream: IO[bytes], need_entire_file_hash: bool) -> Chunker:
		return FastCDCStreamChunker(self._config, stream, need_entire_file_hash)


@dataclasses.dataclass(frozen=True)
class FixedSizeChunkerDefinition(ChunkerDefinition):
	chunk_size: int

	@override
	def create_file_chunker(self, file_path: Path, need_entire_file_hash: bool, *, previous_chunks: Optional[Iterable[PrettyChunk]] = None) -> Chunker:
		return FixedSizeFileChunker(self.chunk_size, file_path, need_entire_file_hash)

	@override
	def create_stream_chunker(self, stream, need_entire_file_hash: bool) -> Chunker:
		return FixedSizeStreamChunker(self.chunk_size, stream, need_entire_file_hash)


@dataclasses.dataclass(frozen=True)
class FixedAutoChunkerDefinition(ChunkerDefinition):
	@override
	def create_file_chunker(self, file_path: Path, need_entire_file_hash: bool, *, previous_chunks: Optional[Iterable[PrettyChunk]] = None) -> Chunker:
		return FixedAutoFileChunker(file_path, previous_chunks, need_entire_file_hash)

	@override
	def create_stream_chunker(self, stream, need_entire_file_hash: bool) -> Chunker:
		return FixedSizeStreamChunker(FixedAutoFileChunker.BIG_CHUNK_SIZE, stream, need_entire_file_hash)

	@override
	def needs_previous_chunks(self) -> bool:
		return True
