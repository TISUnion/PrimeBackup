import enum
import os
from pathlib import Path
from typing import NamedTuple, Optional

from prime_backup.types.common import PathLike
from prime_backup.types.tar_format import TarFormat


class _ZipFormat(NamedTuple):
	extension: str


class StandaloneBackupFormat(enum.Enum):
	tar = TarFormat.plain
	tar_gz = TarFormat.gzip
	tar_xz = TarFormat.lzma
	tar_zst = TarFormat.zstd
	zip = _ZipFormat('.zip')

	@property
	def file_ext(self) -> str:
		if isinstance(self.value, TarFormat):
			return self.value.value.extension
		elif isinstance(self.value, _ZipFormat):
			return self.value.extension
		else:
			raise ValueError(self.value)

	@classmethod
	def from_file_name(cls, file: PathLike) -> Optional['StandaloneBackupFormat']:
		if isinstance(file, Path):
			name = file.name
		else:
			name = os.path.basename(file)

		for ebf in StandaloneBackupFormat:
			if name.endswith(ebf.file_ext):
				return ebf
		return None
