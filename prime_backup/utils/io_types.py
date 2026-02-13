from typing_extensions import Protocol


class SupportsReadBytes(Protocol):
	def read(self, length: int = -1) -> bytes:
		...


class SupportsWriteBytes(Protocol):
	def write(self, s: bytes) -> int:
		...
