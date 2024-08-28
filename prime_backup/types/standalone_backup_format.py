import dataclasses
import enum
import os
from typing import Optional, List

from prime_backup.types.tar_format import TarFormat
from prime_backup.utils.path_like import PathLike


@dataclasses.dataclass(frozen=True)
class ZipFormat:
	extension: str

	@property
	def all_extensions(self) -> List[str]:
		return [self.extension]


class StandaloneBackupFormat(enum.Enum):
	tar = TarFormat.plain
	tar_gz = TarFormat.gzip
	tar_bz2 = TarFormat.bz2
	tar_xz = TarFormat.lzma
	tar_zst = TarFormat.zstd
	zip = ZipFormat('.zip')

	@property
	def __all_file_extensions(self) -> List[str]:
		if isinstance(self.value, TarFormat):
			return self.value.value.all_extensions
		elif isinstance(self.value, ZipFormat):
			return self.value.all_extensions
		else:
			raise ValueError(self.value)

	@classmethod
	def from_file_name(cls, file: PathLike) -> Optional['StandaloneBackupFormat']:
		name = os.path.basename(file)
		for ebf in StandaloneBackupFormat:
			for ext in ebf.__all_file_extensions:
				if name.endswith(ext):
					return ebf
		return None
