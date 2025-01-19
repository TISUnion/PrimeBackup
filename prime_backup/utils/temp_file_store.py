import shutil
from pathlib import Path

from typing_extensions import Self


class TempFileStore:
	def __init__(self, store_path: Path):
		self.__store_path = store_path
		self.__has_closed = False

	def __enter__(self) -> Self:
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.close()

	def close(self):
		if self.__has_closed:
			return
		if self.__store_path.is_dir():
			shutil.rmtree(self.__store_path)
		self.__has_closed = True

	def get_path(self, file_name: str) -> Path:
		if self.__has_closed:
			raise RuntimeError('TempFileStore has already been closed')

		self.__store_path.mkdir(parents=True, exist_ok=True)
		return self.__store_path / file_name
