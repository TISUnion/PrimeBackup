import dataclasses
from pathlib import Path
from typing import List, IO

from prime_backup.types.chunk_method import ChunkMethod
from prime_backup.utils import misc_utils, hash_utils
from prime_backup.utils.chunker import PrettyChunk
from prime_backup.utils.hash_utils import SizeAndHash


@dataclasses.dataclass(frozen=True)
class BlobPrecalculateResult:
	class SizeMismatched(Exception):
		pass

	size: int
	hash: str
	should_be_chunked: bool
	chunks: List[PrettyChunk]

	def simple_repr(self) -> str:
		return misc_utils.represent(self, attrs={
			'size': self.size,
			'hash': self.hash,
			'should_be_chunked': self.should_be_chunked,
			'chunks_len': len(self.chunks),
		})

	@classmethod
	def from_stream(cls, stream: IO[bytes], rel_path: Path, size: int) -> 'BlobPrecalculateResult':
		chunk_method = ChunkMethod.get_for_file(rel_path, size)
		chunks: List[PrettyChunk] = []
		if chunk_method is not None:
			chunker = chunk_method.create_stream_chunker(stream, need_entire_file_hash=True)
			chunks = chunker.cut_all()
			sah = SizeAndHash(chunker.get_read_file_size(), chunker.get_entire_file_hash())
		else:
			sah = hash_utils.calc_reader_size_and_hash(stream)
		if sah.size != size:
			raise cls.SizeMismatched()

		return BlobPrecalculateResult(
			size=sah.size,
			hash=sah.hash,
			should_be_chunked=chunk_method is not None,
			chunks=chunks,
		)

	@classmethod
	def from_file(cls, path: Path, rel_path: Path, size: int) -> 'BlobPrecalculateResult':
		chunk_method = ChunkMethod.get_for_file(rel_path, size)
		chunks: List[PrettyChunk] = []
		if chunk_method is not None:
			chunker = chunk_method.create_file_chunker(path, need_entire_file_hash=True)
			chunks = chunker.cut_all()
			sah = SizeAndHash(chunker.get_read_file_size(), chunker.get_entire_file_hash())
		else:
			sah = hash_utils.calc_file_size_and_hash(path)
		if sah.size != size:
			raise cls.SizeMismatched()

		return BlobPrecalculateResult(
			size=sah.size,
			hash=sah.hash,
			should_be_chunked=chunk_method is not None,
			chunks=chunks,
		)
