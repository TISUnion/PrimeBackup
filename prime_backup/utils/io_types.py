from typing_extensions import Protocol


class SupportsReadBytes(Protocol):
	def read(self, length: int = -1) -> bytes:
		...


class SupportsReadAndSeek(SupportsReadBytes, Protocol):
	def seekable(self) -> bool:
		...

	def seek(self, offset: int, whence: int = 0):
		...


class SupportsWriteBytes(Protocol):
	def write(self, s: bytes) -> int:
		...
