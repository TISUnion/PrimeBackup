import enum
from typing import NamedTuple, Tuple, List

from prime_backup.compressors import CompressMethod


class _TarFormatItem(NamedTuple):
	extension: str
	extra_extensions: Tuple[str, ...]
	mode_extra: str
	compress_method: CompressMethod

	@property
	def all_extensions(self) -> List[str]:
		return [self.extension, *self.extra_extensions]

	@property
	def mode_r(self) -> str:
		return 'r' + self.mode_extra

	@property
	def mode_w(self) -> str:
		return 'w' + self.mode_extra


class TarFormat(enum.Enum):
	plain = _TarFormatItem('.tar', (), ':', CompressMethod.plain)
	gzip = _TarFormatItem('.tar.gz', ('.tgz',), ':gz', CompressMethod.plain)
	lzma = _TarFormatItem('.tar.xz', ('.txz',), ':xz', CompressMethod.plain)
	zstd = _TarFormatItem('.tar.zst', ('.tzst',), ':', CompressMethod.zstd)
