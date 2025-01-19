import contextlib
import os
import shutil
from pathlib import Path
from typing import ContextManager, IO


class _DbFileBackupHelper:
	def __init__(self, src_file: Path, backup_dir: Path, backup_base_name: str):
		self.src_file = src_file
		self.backup_dir = backup_dir
		self.backup_base_name = backup_base_name
		try:
			import zstandard
			self.__has_zstd = True
			self.backup_file = self.backup_dir / (self.backup_base_name + '.zst')
		except ImportError:
			self.__has_zstd = False
			self.backup_file = self.backup_dir / (self.backup_base_name + '.gz')

	@contextlib.contextmanager
	def __open_backup_for_write(self) -> ContextManager[IO[bytes]]:
		self.backup_dir.mkdir(exist_ok=True, parents=True)
		if self.__has_zstd:
			import zstandard
			with zstandard.open(self.backup_file, 'wb') as f_compressed:
				yield f_compressed
		else:
			import gzip
			with gzip.open(self.backup_file, 'wb') as gzf:
				yield gzf

	@contextlib.contextmanager
	def __open_backup_for_read(self) -> ContextManager[IO[bytes]]:
		if self.__has_zstd:
			import zstandard
			with zstandard.open(self.backup_file, 'rb') as f_decompressed:
				yield f_decompressed
		else:
			import gzip
			with gzip.open(self.backup_file, 'rb') as gzf:
				yield gzf

	def create(self, skip_existing: bool):
		if skip_existing and self.backup_file.is_file():
			return
		with open(self.src_file, 'rb') as f_src, self.__open_backup_for_write() as f_dst:
			shutil.copyfileobj(f_src, f_dst)

	def restore(self):
		with open(self.src_file, 'wb') as f_dst, self.__open_backup_for_read() as f_src:
			shutil.copyfileobj(f_src, f_dst)

	def delete_all(self):
		self.backup_file.unlink(missing_ok=True)
		# TODO: auto delete backups once migration done
		for file_name in os.listdir(self.backup_dir):
			file_path = self.backup_dir / file_name