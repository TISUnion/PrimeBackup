import contextlib
import tarfile
from pathlib import Path
from typing import Any, ContextManager

from prime_backup.action import Action
from prime_backup.compressors import Compressor
from prime_backup.db.access import DbAccess
from prime_backup.exceptions import PrimeBackupError
from prime_backup.types.tar_format import TarFormat


class UnsupportedFormat(PrimeBackupError):
	pass


class ImportBackupAction(Action):
	def __init__(self, file_path: Path):
		super().__init__()
		self.file_path = file_path

	@contextlib.contextmanager
	def __open_tar(self, tar_format: TarFormat) -> ContextManager[tarfile.TarFile]:
		with open(self.file_path, 'rb') as f:
			compressor = Compressor.create(tar_format.value.compress_method)
			with compressor.compress_stream(f) as f_compressed:
				with tarfile.open(fileobj=f_compressed, mode=tar_format.value.mode_r) as tar:
					yield tar

	def run(self) -> Any:
		for tar_format in TarFormat:
			if self.file_path.name.endswith(tar_format.value.extension):
				break
		else:
			raise UnsupportedFormat()

		with DbAccess.open_session() as session:
			with self.__open_tar(tar_format) as tar:
				member: tarfile.TarInfo
				for member in tar.getmembers():
					# TODO
					raise NotImplementedError()
