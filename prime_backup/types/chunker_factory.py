from abc import abstractmethod, ABC
from pathlib import Path
from typing import IO

from typing_extensions import override

from prime_backup.utils.chunker import Chunker, CDCFileChunker, CDCStreamChunker, Fixed4KFileChunker, Fixed4KStreamChunker


class ChunkerFactory(ABC):
	@abstractmethod
	def create_file_chunker(self, file_path: Path, need_entire_file_hash: bool) -> Chunker:
		...

	@abstractmethod
	def create_stream_chunker(self, stream, need_entire_file_hash: bool) -> Chunker:
		...


class CDCChunkerFactory(ChunkerFactory):
	@override
	def create_file_chunker(self, file_path: Path, need_entire_file_hash: bool) -> Chunker:
		return CDCFileChunker(file_path, need_entire_file_hash)

	@override
	def create_stream_chunker(self, stream: IO[bytes], need_entire_file_hash: bool) -> Chunker:
		return CDCStreamChunker(stream, need_entire_file_hash)


class Fixed4KChunkerFactory(ChunkerFactory):
	@override
	def create_file_chunker(self, file_path: Path, need_entire_file_hash: bool) -> Chunker:
		return Fixed4KFileChunker(file_path, need_entire_file_hash)

	@override
	def create_stream_chunker(self, stream, need_entire_file_hash: bool) -> Chunker:
		return Fixed4KStreamChunker(stream, need_entire_file_hash)
