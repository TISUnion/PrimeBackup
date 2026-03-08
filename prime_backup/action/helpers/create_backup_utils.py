import contextlib
import enum
import os
from pathlib import Path
from typing import Generator, Literal, BinaryIO

from typing_extensions import Self

from prime_backup.utils.path_like import PathLike


class CreateBackupTimeCostKey(enum.Enum):
	kind_db = enum.auto()
	kind_fs = enum.auto()
	kind_io_read = enum.auto()
	kind_io_write = enum.auto()
	kind_io_copy = enum.auto()

	stage_scan_files = enum.auto()
	stage_reuse_unchanged_files = enum.auto()
	stage_pre_calculate_hash = enum.auto()
	stage_prepare_blob_store = enum.auto()
	stage_create_files = enum.auto()
	stage_finalize = enum.auto()
	stage_flush_db = enum.auto()

	def __lt__(self, other: Self) -> bool:
		return self.name < other.name


class SourceFileNotFoundWrapper(FileNotFoundError):
	def __init__(self, e: FileNotFoundError, file_path: Path):
		super().__init__(e)
		self.file_path = file_path

	@classmethod
	@contextlib.contextmanager
	def wrap(cls, path: PathLike) -> Generator[None, None, None]:
		try:
			yield
		except FileNotFoundError as e:
			if e.filename == os.fspath(path):
				raise cls(e, Path(path))
			else:
				raise

	@classmethod
	def open_rb(cls, path: PathLike, flag: Literal['rb']) -> BinaryIO:
		if flag != 'rb':
			raise ValueError('flag should be rb')
		with cls.wrap(path):
			return open(path, 'rb')


def remove_file(file_to_remove: Path, *, what: str):
	try:
		file_to_remove.unlink(missing_ok=True)
	except OSError as e:
		from prime_backup import logger
		logger.get().error('({}) remove file {!r} failed: {}'.format(what, file_to_remove, e))
