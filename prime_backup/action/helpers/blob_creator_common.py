import contextlib
import dataclasses
import logging
import os
import threading
from abc import ABC
from pathlib import Path
from typing import Callable, ContextManager, Dict, Generator, List, Optional, TYPE_CHECKING, TypeVar, Union

from typing_extensions import NoReturn, override

from prime_backup import logger
from prime_backup.action.helpers import create_backup_utils
from prime_backup.action.helpers.blob_pre_calc_result import BlobPrecalculateResult
from prime_backup.action.helpers.blob_recorder import BlobRecorder
from prime_backup.action.helpers.create_backup_utils import CreateBackupTimeCostKey
from prime_backup.db import schema
from prime_backup.db.session import DbSession
from prime_backup.exceptions import PrimeBackupError
from prime_backup.types.chunker import PrettyChunk
from prime_backup.utils import misc_utils
from prime_backup.utils.time_cost_stats import TimeCostStats

if TYPE_CHECKING:
	from prime_backup.action.helpers.pack_writer import PackWriter

_BLOB_ALLOC_PERF_MODE = False
_T = TypeVar('_T')


class VolatileBlobFile(PrimeBackupError):
	pass


class BlobFileChanged(PrimeBackupError):
	pass


@dataclasses.dataclass(frozen=True)
class LookupBlobBySizeRequest:
	size: int


@dataclasses.dataclass(frozen=True)
class LookupBlobByHashRequest:
	hash: str


BlobLookupRequest = Union[LookupBlobBySizeRequest, LookupBlobByHashRequest]
BlobLookupRoutine = Generator[BlobLookupRequest, None, _T]


class _FailureFileDeleter(ContextManager['_FailureFileDeleter']):
	def __init__(self, what: str = 'failure_delete'):
		self.file_paths: List[Path] = []
		self.what = what

	def mark(self, path: Path):
		self.file_paths.append(path)

	@override
	def __enter__(self) -> '_FailureFileDeleter':
		return self

	@override
	def __exit__(self, exc_type, exc_val, exc_tb):
		if exc_type is not None:
			logger.get().debug(f'Deleting {len(self.file_paths)} files due to exception {exc_val}: {self.file_paths}')
			for file_path in self.file_paths:
				create_backup_utils.remove_file(file_path, what=self.what)


@dataclasses.dataclass
class BlobCreateContext:
	session: DbSession
	time_costs: TimeCostStats[CreateBackupTimeCostKey]
	blob_recorder: BlobRecorder
	source_path: Path
	temp_path: Path
	pre_calc_result_getter: Callable[[Path], Optional[BlobPrecalculateResult]]
	previous_chunks_getter: Callable[[Path], Optional[List[PrettyChunk]]]
	pack_writer: 'PackWriter'
	blob_by_size_cache: Dict[int, bool]
	blob_by_hash_cache: Dict[str, schema.Blob]
	blob_store_st: Optional[os.stat_result] = None
	blob_store_in_cow_fs: Optional[bool] = None

	@contextlib.contextmanager
	def make_temp_file(self, src_path_md5: str) -> Generator[Path, None, None]:
		temp_file_name = f'blob_{os.getpid()}_{threading.current_thread().ident}_{src_path_md5}.tmp'
		temp_file_path = self.temp_path / temp_file_name
		try:
			yield temp_file_path
		finally:
			create_backup_utils.remove_file(temp_file_path, what='temp_file')

	def get_cached_blob(self, blob_hash: str) -> Optional[schema.Blob]:
		return self.blob_by_hash_cache.get(blob_hash)

	def remember_blob(self, blob: schema.Blob):
		self.blob_by_size_cache[blob.raw_size] = True
		self.blob_by_hash_cache[blob.hash] = blob


class BlobCreatorBase(ABC):
	def __init__(self, context: BlobCreateContext):
		self.ctx = context

		from prime_backup import logger
		from prime_backup.config.config import Config
		self.logger: logging.Logger = logger.get()
		self.config: Config = Config.get()

	def log_and_raise_blob_file_changed(self, msg: str, last_chance: bool) -> NoReturn:
		(self.logger.warning if last_chance else self.logger.debug)(msg)
		raise BlobFileChanged(msg)

	def query_cached_blob(self, blob_hash: str) -> BlobLookupRoutine[Optional[schema.Blob]]:
		if (cache := self.ctx.get_cached_blob(blob_hash)) is not None:
			return cache
		yield LookupBlobByHashRequest(blob_hash)
		return self.ctx.get_cached_blob(blob_hash)

	def query_blob_size_exists(self, blob_size: int) -> BlobLookupRoutine[bool]:
		if (exist := self.ctx.blob_by_size_cache.get(blob_size)) is not None:
			return exist
		yield LookupBlobBySizeRequest(blob_size)
		return misc_utils.ensure_type(self.ctx.blob_by_size_cache[blob_size], bool)
