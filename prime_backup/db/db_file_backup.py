import contextlib
from pathlib import Path
from typing import Generator

from prime_backup.utils import file_utils
from prime_backup.utils.io_types import SupportsReadBytes, SupportsWriteBytes


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
	def __open_backup_for_write(self) -> Generator[SupportsWriteBytes, None, None]:
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
	def __open_backup_for_read(self) -> Generator[SupportsReadBytes, None, None]:
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
			file_utils.copy_file_obj_fast(f_src, f_dst)

	def restore(self):
		tmp_src_file = self.src_file.with_name(self.src_file.name + '.tmp')
		try:
			with open(tmp_src_file, 'wb') as f_dst, self.__open_backup_for_read() as f_src:
				file_utils.copy_file_obj_fast(f_src, f_dst)
			tmp_src_file.replace(self.src_file)
		except Exception:
			tmp_src_file.unlink(missing_ok=True)
			raise

	def delete_all(self):
		self.backup_file.unlink(missing_ok=True)
