import dataclasses
import enum
from typing import Tuple, List

from prime_backup.compressors import CompressMethod


@dataclasses.dataclass(frozen=True)
class _TarFormatItem:
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
	bz2 = _TarFormatItem('.tar.bz2', ('.tbz2',), ':bz2', CompressMethod.plain)
	lzma = _TarFormatItem('.tar.xz', ('.txz',), ':xz', CompressMethod.plain)
	zstd = _TarFormatItem('.tar.zst', ('.tar.zstd', '.tzst', '.tzstd'), ':', CompressMethod.zstd)


def __validate_tar_formats():
	for tf in TarFormat:
		for ext in tf.value.all_extensions:
			if not ext.startswith('.'):
				raise AssertionError('bad extension that does not start with "." for {}: {}'.format(tf, ext))


__validate_tar_formats()
