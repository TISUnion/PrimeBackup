import dataclasses


@dataclasses.dataclass(frozen=True)
class SizeDiff:
	before: int
	after: int

	@property
	def diff(self) -> int:
		return self.after - self.before
