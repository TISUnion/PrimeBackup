import contextlib
import enum
from pathlib import Path
from typing import Generator, Literal, BinaryIO, TYPE_CHECKING, List

from typing_extensions import Self

from prime_backup.action.helpers.fileset_allocator import FilesetAllocateArgs, FilesetAllocator
from prime_backup.db import schema
from prime_backup.db.session import DbSession
from prime_backup.utils.path_like import PathLike


if TYPE_CHECKING:
	from prime_backup.config.config import Config


class TimeCostKey(enum.Enum):
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
			raise cls(e, Path(path))

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


def finalize_backup_and_files(config: 'Config', session: DbSession, backup: schema.Backup, files: List[schema.File]):
	allocate_args = FilesetAllocateArgs.from_config(config)
	allocate_result = FilesetAllocator(session, files).allocate(allocate_args)
	fs_base, fs_delta = allocate_result.fileset_base, allocate_result.fileset_delta

	backup.fileset_id_base = fs_base.id
	backup.fileset_id_delta = fs_delta.id
	backup.file_count = fs_base.file_count + fs_delta.file_count
	backup.file_raw_size_sum = fs_base.file_raw_size_sum + fs_delta.file_raw_size_sum
	backup.file_stored_size_sum = fs_base.file_stored_size_sum + fs_delta.file_stored_size_sum

	session.add(backup)
	session.flush()  # this generates backup.id
