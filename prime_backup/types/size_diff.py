from typing import NamedTuple


class SizeDiff(NamedTuple):
	before: int
	after: int

	@property
	def diff(self) -> int:
		return self.after - self.before
