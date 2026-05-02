import dataclasses
import enum
import os
from typing import Optional, List, Union, TYPE_CHECKING

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

	if TYPE_CHECKING:
		value: Union[TarFormat, ZipFormat]

	@property
	def __all_file_extensions(self) -> List[str]:
		format_value = self.value
		if isinstance(format_value, TarFormat):
			return format_value.value.all_extensions
		elif isinstance(format_value, ZipFormat):
			return format_value.all_extensions
		else:
			raise ValueError(self.value)

	@classmethod
	def from_file_name(cls, file: PathLike) -> Optional['StandaloneBackupFormat']:
		name_lower = os.path.basename(file).lower()
		for ebf in StandaloneBackupFormat:
			for ext in ebf.__all_file_extensions:
				if name_lower.endswith(ext):
					return ebf
		return None
