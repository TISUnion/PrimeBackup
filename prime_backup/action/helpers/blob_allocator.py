import collections
import dataclasses
import hashlib
import logging
import os
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Callable, Dict, Set, Deque, TYPE_CHECKING, Union

from typing_extensions import override

from prime_backup.action.helpers.blob_creator_chunked import ChunkedBlobCreator
from prime_backup.action.helpers.blob_creator_common import BlobCreateContext, BlobFileChanged, VolatileBlobFile, LookupBlobBySizeRequest, LookupBlobByHashRequest, BlobLookupRequest, BlobLookupRoutine, _BLOB_ALLOC_PERF_MODE
from prime_backup.action.helpers.blob_creator_direct import DirectBlobCreator
from prime_backup.action.helpers.blob_pre_calc_result import BlobPrecalculateResult
from prime_backup.action.helpers.blob_recorder import BlobRecorder
from prime_backup.action.helpers.create_backup_utils import CreateBackupTimeCostKey, SourceFileNotFoundWrapper
from prime_backup.db import schema
from prime_backup.db.session import DbSession
from prime_backup.types.chunk_method import ChunkMethod
from prime_backup.types.chunker import PrettyChunk
from prime_backup.utils import blob_utils, file_utils, misc_utils, pack_utils
from prime_backup.utils.time_cost_stats import TimeCostStats

if TYPE_CHECKING:
	from prime_backup.action.helpers.pack_writer import PackWriter


_BLOB_FILE_CHANGED_RETRY_COUNT = 3
_FetchCallback = Callable[[], None]


class BatchFetcherBase(ABC):
	def __init__(self, session: DbSession, max_batch_size: int, time_costs: TimeCostStats[CreateBackupTimeCostKey]):
		self.session = session
		self.max_batch_size = max_batch_size
		self.first_task_scheduled_time = time.time()
		self.time_costs = time_costs

	def _post_query(self):
		now = time.time()
		if self._task_count() == 1:
			self.first_task_scheduled_time = now
		self.flush_if_needed()

	def flush_if_needed(self):
		if self._task_count() > 0 and (self._task_count() >= self.max_batch_size or time.time() - self.first_task_scheduled_time >= 0.1):
			self._batch_run()

	def flush(self):
		if self._task_count() > 0:
			self._batch_run()

	@abstractmethod
	def _task_count(self) -> int:
		...

	@abstractmethod
	def _batch_run(self):
		...


class BlobBySizeFetcher(BatchFetcherBase):
	def __init__(self, session: DbSession, max_batch_size: int, result_store: Dict[int, bool], time_costs: TimeCostStats[CreateBackupTimeCostKey]):
		super().__init__(session, max_batch_size, time_costs)
		self.__callbacks: List[_FetchCallback] = []
		self.__sizes: Set[int] = set()
		self.__result_store = result_store

	def query(self, query: LookupBlobBySizeRequest, callback: _FetchCallback):
		self.__callbacks.append(callback)
		self.__sizes.add(query.size)
		self._post_query()

	@override
	def _task_count(self) -> int:
		return len(self.__callbacks)

	@override
	def _batch_run(self):
		with self.time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_db):
			existence = self.session.has_blob_with_size_batched(list(self.__sizes))
		self.__result_store.update(existence)
		# reverse since we want to keep the file order, and collections.deque.appendleft is FILO
		for callback in reversed(self.__callbacks):
			callback()
		self.__callbacks.clear()
		self.__sizes.clear()


class BlobByHashFetcher(BatchFetcherBase):
	def __init__(self, session: DbSession, max_batch_size: int, result_store: Dict[str, schema.Blob], time_costs: TimeCostStats[CreateBackupTimeCostKey]):
		super().__init__(session, max_batch_size, time_costs)
		self.__callbacks: List[_FetchCallback] = []
		self.__hashes: Set[str] = set()
		self.__result_store = result_store

	def query(self, query: LookupBlobByHashRequest, callback: _FetchCallback):
		self.__callbacks.append(callback)
		self.__hashes.add(query.hash)
		self._post_query()

	@override
	def _task_count(self) -> int:
		return len(self.__callbacks)

	@override
	def _batch_run(self):
		with self.time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_db):
			blobs = self.session.get_blobs_by_hashes_opt(list(self.__hashes))
		for blob_hash, blob in blobs.items():
			if blob is not None:
				self.__result_store[blob_hash] = blob
		# reverse since we want to keep the file order, and collections.deque.appendleft is FILO
		for callback in reversed(self.__callbacks):
			callback()
		self.__callbacks.clear()
		self.__hashes.clear()


class BatchLookupManager:
	def __init__(self, session: DbSession, size_result_store: Dict[int, bool], hash_result_store: Dict[str, schema.Blob], time_costs: TimeCostStats[CreateBackupTimeCostKey], *, max_batch_size: int = 100):
		self.fetcher_size = BlobBySizeFetcher(session, max_batch_size, size_result_store, time_costs)
		self.fetcher_hash = BlobByHashFetcher(session, max_batch_size, hash_result_store, time_costs)

	def query(self, query: BlobLookupRequest, callback: _FetchCallback):
		if isinstance(query, LookupBlobBySizeRequest):
			self.fetcher_size.query(query, callback)
		elif isinstance(query, LookupBlobByHashRequest):
			self.fetcher_hash.query(query, callback)
		else:
			raise TypeError('unexpected query: {!r} {!r}'.format(type(query), query))

	def flush_if_needed(self):
		self.fetcher_size.flush_if_needed()
		self.fetcher_hash.flush_if_needed()

	def flush(self):
		self.fetcher_size.flush()
		self.fetcher_hash.flush()


@dataclasses.dataclass(frozen=True)
class GetOrCreateBlobResult:
	blob: schema.Blob
	st: os.stat_result


class BlobAllocator:
	def __init__(
			self,
			session: DbSession,
			time_costs: TimeCostStats[CreateBackupTimeCostKey],
			blob_recorder: BlobRecorder,
			source_path: Path,
			temp_path: Path,
			pre_calc_result_getter: Callable[[Path], Optional[BlobPrecalculateResult]],
			previous_chunks_getter: Callable[[Path], Optional[List[PrettyChunk]]],
			pack_writer: 'PackWriter',
	):
		from prime_backup import logger
		from prime_backup.config.config import Config
		self.logger: logging.Logger = logger.get()
		self.config: Config = Config.get()

		self.session = session
		self.__time_costs = time_costs
		self.__source_path = source_path
		self.__blob_by_size_cache: Dict[int, bool] = {}
		self.__blob_by_hash_cache: Dict[str, schema.Blob] = {}
		self.__batch_lookup_manager = BatchLookupManager(session, self.__blob_by_size_cache, self.__blob_by_hash_cache, time_costs)

		self.__ctx = BlobCreateContext(
			session=session,
			time_costs=time_costs,
			blob_recorder=blob_recorder,
			source_path=source_path,
			temp_path=temp_path,
			pre_calc_result_getter=pre_calc_result_getter,
			previous_chunks_getter=previous_chunks_getter,
			pack_writer=pack_writer,
			blob_by_size_cache=self.__blob_by_size_cache,
			blob_by_hash_cache=self.__blob_by_hash_cache,
		)

	def __should_skip_missing_source_file(self, src_file_path: Path) -> bool:
		if self.config.backup.creation_skip_missing_file:
			try:
				rel_path = src_file_path.relative_to(self.__source_path)
			except ValueError:
				self.logger.error("Path {!r} is not inside the source path {!r}".format(str(src_file_path), str(self.__source_path)))
			else:
				return self.config.backup.creation_skip_missing_file_patterns_spec.match_file(rel_path)
		return False

	def __get_chunk_method(self, file_path: Path, file_size: int) -> Optional[ChunkMethod]:
		try:
			rel_path = file_path.relative_to(self.__source_path)
		except ValueError:
			self.logger.error("Path {!r} is not inside the source path {!r}".format(str(file_path), str(self.__source_path)))
			return None
		else:
			return ChunkMethod.get_for_file(rel_path, file_size)

	def __is_mutating_file(self, file_path: Path) -> bool:
		try:
			rel_path = file_path.relative_to(self.__source_path)
		except ValueError:
			self.logger.error("Path {!r} is not inside the source path {!r}".format(str(file_path), str(self.__source_path)))
			return False
		else:
			return self.config.backup.mutating_file_patterns_spec.match_file(rel_path)

	def __try_get_or_create_blob_once(self, src_path: Path, src_path_md5: str, st: os.stat_result, last_chance: bool, is_mutating_file: bool) -> BlobLookupRoutine[schema.Blob]:
		chunk_method = self.__get_chunk_method(src_path, st.st_size)
		creator: Union[ChunkedBlobCreator, DirectBlobCreator]
		if chunk_method is not None:
			creator = ChunkedBlobCreator(self.__ctx, ChunkedBlobCreator.Args(
				src_path=src_path,
				src_path_md5=src_path_md5,
				st=st,
				chunk_method=chunk_method,
				last_chance=last_chance,
				is_mutating_file=is_mutating_file,
			))
		else:
			creator = DirectBlobCreator(self.__ctx, DirectBlobCreator.Args(
				src_path=src_path,
				src_path_md5=src_path_md5,
				st=st,
				last_chance=last_chance,
				is_mutating_file=is_mutating_file,
			))

		return (yield from creator.get_or_create())

	def get_or_create_blob(self, src_path: Path, st: os.stat_result) -> BlobLookupRoutine[GetOrCreateBlobResult]:
		src_path_str = repr(src_path.as_posix())
		src_path_md5 = hashlib.md5(src_path_str.encode('utf8')).hexdigest()
		is_mutating_file = self.__is_mutating_file(src_path)

		for i in range(_BLOB_FILE_CHANGED_RETRY_COUNT):
			retry_cnt = i + 1  # [1, n]
			is_last_attempt = retry_cnt == _BLOB_FILE_CHANGED_RETRY_COUNT
			if i > 0:
				self.logger.debug('Try to create blob {} (attempt {} / {})'.format(src_path_str, retry_cnt, _BLOB_FILE_CHANGED_RETRY_COUNT))
			try:
				blob = yield from self.__try_get_or_create_blob_once(src_path, src_path_md5, st, last_chance=is_last_attempt, is_mutating_file=is_mutating_file)
				self.__ctx.remember_blob(blob)
				return GetOrCreateBlobResult(blob, st)
			except BlobFileChanged:
				(self.logger.warning if is_last_attempt else self.logger.debug)('Blob {} stat has changed, has someone modified it? {} (attempt {} / {})'.format(
					src_path_str, 'No more retry' if is_last_attempt else 'Retrying', retry_cnt, _BLOB_FILE_CHANGED_RETRY_COUNT
				))
				st = src_path.lstat()
			except Exception as e:
				self.logger.error('Create blob for file {} failed (attempt {} / {}): {}'.format(src_path_str, retry_cnt, _BLOB_FILE_CHANGED_RETRY_COUNT, e))
				raise

		self.logger.error('All blob copy attempts failed since the file {} keeps changing'.format(src_path_str))
		raise VolatileBlobFile('blob file {} keeps changing'.format(src_path_str))

	def init_blob_store(self):
		with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.stage_prepare_blob_store, CreateBackupTimeCostKey.kind_fs):
			blob_utils.prepare_blob_directories()
			pack_utils.prepare_pack_directories()

			bs_path = blob_utils.get_blob_store()
			self.__ctx.blob_store_st = bs_path.stat()
			self.__ctx.blob_store_in_cow_fs = file_utils.does_fs_support_cow(bs_path)

	def schedule_loop(self, gen_list: List[BlobLookupRoutine[schema.File]]) -> List[schema.File]:
		if _BLOB_ALLOC_PERF_MODE:
			return self.__schedule_loop_simple(gen_list)

		files: List[schema.File] = []

		schedule_queue: Deque[BlobLookupRoutine[schema.File]] = collections.deque()
		for gen in gen_list:
			schedule_queue.append(gen)
		gen_count = len(schedule_queue)
		done_count = 0

		start_time = time.time()
		last_report_time = start_time

		while len(schedule_queue) > 0:
			scheduled = schedule_queue.popleft()
			try:
				def callback(g: BlobLookupRoutine[schema.File] = scheduled):
					schedule_queue.appendleft(g)

				query_req = scheduled.send(None)
				self.__batch_lookup_manager.query(query_req, callback)
			except StopIteration as e:
				files.append(misc_utils.ensure_type(e.value, schema.File))
				done_count += 1
				if (now := time.time()) - last_report_time > 30:
					self.logger.info('Backup file creation progress: {} / {} done, elapsed time {:.2f}s'.format(done_count, gen_count, now - start_time))
					last_report_time = now
			except SourceFileNotFoundWrapper as e:
				if self.__should_skip_missing_source_file(e.file_path):
					self.logger.warning('Backup source file {!r} not found, suppressed and skipped by config'.format(str(e.file_path)))
				else:
					raise

			self.__batch_lookup_manager.flush_if_needed()
			if len(schedule_queue) == 0:
				self.__batch_lookup_manager.flush()

		return files

	def __schedule_loop_simple(self, gen_list: List[BlobLookupRoutine[schema.File]]) -> List[schema.File]:
		files: List[schema.File] = []

		def gen_wrapper(g: BlobLookupRoutine[schema.File]):
			f = yield from g
			files.append(f)

		for gen in gen_list:
			for query_req in gen_wrapper(gen):
				self.__batch_lookup_manager.query(query_req, lambda: None)
				self.__batch_lookup_manager.flush()
		return files


	def add_existing_sizes(self, existing_sizes: Dict[int, bool]):
		self.__blob_by_size_cache.update(existing_sizes)
