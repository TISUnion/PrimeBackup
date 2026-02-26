import dataclasses
from typing import List

from prime_backup.utils import misc_utils
from prime_backup.utils.chunk_utils import PrettyChunk


@dataclasses.dataclass(frozen=True)
class BlobPrecalculateResult:
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
