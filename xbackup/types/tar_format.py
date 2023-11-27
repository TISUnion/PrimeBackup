import enum
from typing import NamedTuple

from xbackup.compressors import CompressMethod


class _TarFormatItem(NamedTuple):
	extension: str
	mode_extra: str
	compress_method: CompressMethod

	@property
	def mode_r(self) -> str:
		return 'r' + self.mode_extra

	@property
	def mode_w(self) -> str:
		return 'w' + self.mode_extra


class TarFormat(enum.Enum):
	plain = _TarFormatItem('.tar', '', CompressMethod.plain)
	gzip = _TarFormatItem('.tar.gz', ':gz', CompressMethod.plain)
	lzma = _TarFormatItem('.tar.xz', ':xz', CompressMethod.plain)
	zstd = _TarFormatItem('.tar.zst', '', CompressMethod.zstd)
