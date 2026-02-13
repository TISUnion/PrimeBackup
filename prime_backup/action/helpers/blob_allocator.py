import collections
import contextlib
import dataclasses
import enum
import functools
import hashlib
import logging
import os
import threading
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Tuple, Callable, Any, Dict, Generator, Union, Set, overload, Deque

import pathspec
from typing_extensions import NoReturn, override

from prime_backup.action.helpers import create_backup_utils
from prime_backup.action.helpers.blob_recorder import BlobRecorder
from prime_backup.action.helpers.chunk_grouper import ChunkGrouper
from prime_backup.action.helpers.create_backup_utils import TimeCostKey, SourceFileNotFoundWrapper
from prime_backup.compressors import Compressor, CompressMethod
from prime_backup.db import schema
from prime_backup.db.session import DbSession
from prime_backup.db.values import BlobStorageMethod
from prime_backup.exceptions import PrimeBackupError
from prime_backup.types.units import ByteCount
from prime_backup.utils import hash_utils, misc_utils, blob_utils, file_utils, chunk_utils
from prime_backup.utils.hash_utils import SizeAndHash
from prime_backup.utils.time_cost_stats import TimeCostStats


class VolatileBlobFile(PrimeBackupError):
	pass


class _BlobFileChanged(PrimeBackupError):
	pass


class _DirectBlobCreatePolicy(enum.Enum):
	"""
	the policy of how to create a blob from a given file path
	"""
	read_all = enum.auto()   # small files: read all in memory, calc hash                                |  read 1x, write 1x
	hash_once = enum.auto()  # files with unique size: compress+hash to temp file, then move             |  read 1x, write 1x, move 1x
	copy_hash = enum.auto()  # files that keep changing: copy to temp file, calc hash, compress to blob  |  read 2x, write 2x. need more spaces
	default = enum.auto()    # default policy: hash and check, then compress+hash to blob store          |  read 2x, write 1x


_BLOB_FILE_CHANGED_RETRY_COUNT = 3
_READ_ALL_SIZE_THRESHOLD = 8 * 1024  # 8KiB
_HASH_ONCE_SIZE_THRESHOLD = 10 * 1024 * 1024  # 10MiB


class _ChunkedBlobCreatePolicy(enum.Enum):
	copy_hash = enum.auto()  # files that keep changing: copy to temp file, calc hash, compress to blob  |  read 2x, write 2x. need more spaces
	default = enum.auto()    # default policy: hash+chunking and check, then chunking+compress+hash to blob store          |  read 2x, write 1x


class BatchFetcherBase(ABC):
	Callback = Callable
	tasks: dict

	def __init__(self, session: DbSession, max_batch_size: int, time_costs: TimeCostStats[TimeCostKey]):
		self.session = session
		self.max_batch_size = max_batch_size
		self.first_task_scheduled_time = time.time()
		self.time_costs = time_costs

	def _post_query(self):
		now = time.time()
		if len(self.tasks) == 1:
			self.first_task_scheduled_time = now
		self.flush_if_needed()

	def flush_if_needed(self):
		if len(self.tasks) > 0 and (len(self.tasks) >= self.max_batch_size or time.time() - self.first_task_scheduled_time >= 0.1):
			self._batch_run()

	def flush(self):
		if len(self.tasks) > 0:
			self._batch_run()

	@abstractmethod
	def _batch_run(self):
		...


class BlobBySizeFetcher(BatchFetcherBase):
	@dataclasses.dataclass(frozen=True)
	class Req:
		size: int

	@dataclasses.dataclass(frozen=True)
	class Rsp:
		exists: bool

	Callback = Callable[[Rsp], None]
	tasks: Dict[int, List[Callback]]

	def __init__(self, session: DbSession, max_batch_size: int, result_cache: Dict[int, bool], time_costs: TimeCostStats[TimeCostKey]):
		super().__init__(session, max_batch_size, time_costs)
		self.tasks: List[Tuple[int, BlobBySizeFetcher.Callback]] = []
		self.sizes: Set[int] = set()
		self.result_cache = result_cache

	def query(self, query: Req, callback: Callback):
		self.tasks.append((query.size, callback))
		self.sizes.add(query.size)
		self._post_query()

	@override
	def _batch_run(self):
		with self.time_costs.measure_time_cost(TimeCostKey.kind_db):
			existence = self.session.has_blob_with_size_batched(list(self.sizes))
		self.result_cache.update(existence)
		# reverse since we want to keep the file order, and collections.deque.appendleft is FILO
		for sz, callback in reversed(self.tasks):
			callback(self.Rsp(existence[sz]))
		self.tasks.clear()
		self.sizes.clear()


class BlobByHashFetcher(BatchFetcherBase):
	@dataclasses.dataclass(frozen=True)
	class Req:
		hash: str

	@dataclasses.dataclass(frozen=True)
	class Rsp:
		blob: Optional[schema.Blob]

	Callback = Callable[[Rsp], None]
	tasks: Dict[str, List[Callback]]

	def __init__(self, session: DbSession, max_batch_size: int, result_cache: Dict[str, schema.Blob], time_costs: TimeCostStats[TimeCostKey]):
		super().__init__(session, max_batch_size, time_costs)
		self.tasks: List[Tuple[str, BlobByHashFetcher.Callback]] = []
		self.hashes: Set[str] = set()
		self.result_cache = result_cache

	def query(self, query: Req, callback: Callback):
		self.tasks.append((query.hash, callback))
		self.hashes.add(query.hash)
		self._post_query()

	@override
	def _batch_run(self):
		with self.time_costs.measure_time_cost(TimeCostKey.kind_db):
			blobs = self.session.get_blobs_by_hashes(list(self.hashes))
		self.result_cache.update(blobs)
		# reverse since we want to keep the file order, and collections.deque.appendleft is FILO
		for h, callback in reversed(self.tasks):
			callback(self.Rsp(blobs[h]))
		self.tasks.clear()
		self.hashes.clear()


BqmReq = Union[BlobBySizeFetcher.Req, BlobByHashFetcher.Req]
BqmRsp = Union[BlobBySizeFetcher.Rsp, BlobByHashFetcher.Rsp]


class BatchQueryManager:
	def __init__(self, session: DbSession, size_result_cache: dict, hash_result_cache: dict, time_costs: TimeCostStats[TimeCostKey], *, max_batch_size: int = 100):
		self.fetcher_size = BlobBySizeFetcher(session, max_batch_size, size_result_cache, time_costs)
		self.fetcher_hash = BlobByHashFetcher(session, max_batch_size, hash_result_cache, time_costs)

	@overload
	def query(self, query: BlobBySizeFetcher.Req, callback: Callable[[BlobBySizeFetcher.Rsp], None]): ...
	@overload
	def query(self, query: BlobByHashFetcher.Req, callback: Callable[[BlobByHashFetcher.Rsp], None]): ...

	def query(self,query: BqmReq, callback: Callable[[BqmRsp], None]):
		if isinstance(query, BlobBySizeFetcher.Req):
			self.fetcher_size.query(query, callback)
		elif isinstance(query, BlobByHashFetcher.Req):
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
			time_costs: TimeCostStats[TimeCostKey],
			blob_recorder: BlobRecorder,
			source_path: Path,
			temp_path: Path,
			pre_calc_hash_getter: Callable[[Path], Optional[str]]
	):
		from prime_backup import logger
		from prime_backup.config.config import Config
		self.logger: logging.Logger = logger.get()
		self.config: Config = Config.get()

		self.session = session
		self.__time_costs = time_costs
		self.__blob_recorder = blob_recorder
		self.__source_path = source_path
		self.__temp_path = temp_path
		self.__pre_calc_hash_getter = pre_calc_hash_getter

		self.__blob_by_size_cache: Dict[int, bool] = {}
		self.__blob_by_hash_cache: Dict[str, schema.Blob] = {}
		self.__batch_query_manager = BatchQueryManager(session, self.__blob_by_size_cache, self.__blob_by_hash_cache, time_costs)

		self.__blob_store_st: Optional[os.stat_result] = None
		self.__blob_store_in_cow_fs: Optional[bool] = None

	@contextlib.contextmanager
	def __make_temp_file(self, src_path_md5: str) -> Generator[Path, None, None]:
		temp_file_name = f'blob_{os.getpid()}_{threading.current_thread().ident}_{src_path_md5}.tmp'
		temp_file_path = self.__temp_path / temp_file_name
		try:
			yield temp_file_path
		finally:
			create_backup_utils.remove_file(temp_file_path, what='temp_file')

	def __log_and_raise_blob_file_changed(self, msg: str, last_chance: bool) -> NoReturn:
		(self.logger.warning if last_chance else self.logger.debug)(msg)
		raise _BlobFileChanged(msg)

	def __try_get_or_create_direct_blob(self, src_path: Path, src_path_md5: str, st: os.stat_result, last_chance: bool) -> Generator[Any, Any, schema.Blob]:
		log_and_raise_blob_file_changed = functools.partial(self.__log_and_raise_blob_file_changed, last_chance=last_chance)

		compress_method: CompressMethod = self.config.backup.get_compress_method_from_size(st.st_size)
		can_copy_on_write = (
				file_utils.HAS_COPY_FILE_RANGE and
				compress_method == CompressMethod.plain and
				self.__blob_store_in_cow_fs and
				st.st_dev == self.__blob_store_st.st_dev
		)

		policy: Optional[_DirectBlobCreatePolicy] = None
		blob_hash: Optional[str] = None
		blob_content: Optional[bytes] = None
		raw_size: Optional[int] = None
		stored_size: Optional[int] = None
		pre_calc_blob_hash = self.__pre_calc_hash_getter(src_path)

		if last_chance:
			policy = _DirectBlobCreatePolicy.copy_hash
		elif pre_calc_blob_hash is not None:  # hash already calculated? just use default
			policy = _DirectBlobCreatePolicy.default
			blob_hash = pre_calc_blob_hash
		elif not can_copy_on_write:  # do tricks iff. no COW copy
			if st.st_size <= _READ_ALL_SIZE_THRESHOLD:
				policy = _DirectBlobCreatePolicy.read_all
				with self.__time_costs.measure_time_cost(TimeCostKey.kind_io_read):
					with SourceFileNotFoundWrapper.open_rb(src_path, 'rb') as f:
						blob_content = f.read(_READ_ALL_SIZE_THRESHOLD + 1)
				if len(blob_content) > _READ_ALL_SIZE_THRESHOLD:
					log_and_raise_blob_file_changed('Read too many bytes for read_all policy, stat: {}, read: {}'.format(st.st_size, len(blob_content)))
				blob_hash = hash_utils.calc_bytes_hash(blob_content)
			elif st.st_size > _HASH_ONCE_SIZE_THRESHOLD:
				if (exist := self.__blob_by_size_cache.get(st.st_size)) is None:
					# existence is unknown yet
					yield BlobBySizeFetcher.Req(st.st_size)
					can_hash_once = self.__blob_by_size_cache[st.st_size] is False
				else:
					can_hash_once = exist is False
				if can_hash_once:
					# it's certain that this blob is unique, but notes: the following code
					# cannot be interrupted (yield), or other generator could make a same blob
					policy = _DirectBlobCreatePolicy.hash_once
		if policy is None:
			policy = _DirectBlobCreatePolicy.default
			with self.__time_costs.measure_time_cost(TimeCostKey.kind_io_read):
				with SourceFileNotFoundWrapper.wrap(src_path):
					blob_hash = hash_utils.calc_file_hash(src_path)

		# self.logger.info("%s %s %s", policy.name, compress_method.name, src_path)
		if blob_hash is not None:
			misc_utils.assert_true(policy != _DirectBlobCreatePolicy.hash_once, 'unexpected policy')

			if (cache := self.__blob_by_hash_cache.get(blob_hash)) is not None:
				return cache
			yield BlobByHashFetcher.Req(blob_hash)
			if (cache := self.__blob_by_hash_cache.get(blob_hash)) is not None:
				return cache

		# notes: the following code cannot be interrupted (yield).
		# The blob is specifically generated by the generator
		# if any yield is done, ensure to check __blob_by_hash_cache again

		def check_changes(new_size: int, new_hash: Optional[str]):
			if new_size != st.st_size:  # XXX: is it really useful?
				log_and_raise_blob_file_changed('Blob size mismatch, previous: {}, current: {}'.format(st.st_size, new_size))
			if blob_hash is not None and new_hash is not None and new_hash != blob_hash:
				log_and_raise_blob_file_changed('Blob hash mismatch, previous: {}, current: {}'.format(blob_hash, new_hash))

		def get_blob_path_for_write(h: str) -> Path:
			"""
			Get blob path by hash, and add the blob path to the rollbacker
			Commonly used right before creating the blob file
			"""
			bp = blob_utils.get_blob_path(h)
			self.__blob_recorder.add_remove_file_rollbacker(bp)
			return bp

		compressor = Compressor.create(compress_method)
		if policy == _DirectBlobCreatePolicy.copy_hash:
			# copy to temp file, calc hash, then compress to blob store
			misc_utils.assert_true(blob_hash is None, 'blob_hash should not be calculated')
			with self.__make_temp_file(src_path_md5) as temp_file_path:
				with self.__time_costs.measure_time_cost(TimeCostKey.kind_io_copy):
					file_utils.copy_file_fast(src_path, temp_file_path, open_r_func=SourceFileNotFoundWrapper.open_rb)
				with self.__time_costs.measure_time_cost(TimeCostKey.kind_io_read):
					tmp_size, blob_hash = hash_utils.calc_file_size_and_hash(temp_file_path)

				misc_utils.assert_true(last_chance, 'only last_chance=True is allowed for the copy_hash policy')
				if (cache := self.__blob_by_hash_cache.get(blob_hash)) is not None:
					return cache
				yield BlobByHashFetcher.Req(blob_hash)
				if (cache := self.__blob_by_hash_cache.get(blob_hash)) is not None:
					return cache

				blob_path = get_blob_path_for_write(blob_hash)
				with self.__time_costs.measure_time_cost(TimeCostKey.kind_io_copy):
					cr = compressor.copy_compressed(temp_file_path, blob_path, calc_hash=False)
				raw_size, stored_size = cr.read_size, cr.write_size

		elif policy == _DirectBlobCreatePolicy.hash_once:
			# read once, compress+hash to temp file, then move
			misc_utils.assert_true(blob_hash is None, 'blob_hash should not be calculated')
			with self.__make_temp_file(src_path_md5) as temp_file_path:
				with self.__time_costs.measure_time_cost(TimeCostKey.kind_io_copy):
					cr = compressor.copy_compressed(src_path, temp_file_path, calc_hash=True, open_r_func=SourceFileNotFoundWrapper.open_rb)
				check_changes(cr.read_size, None)  # the size must be unchanged, to satisfy the uniqueness

				raw_size, blob_hash, stored_size = cr.read_size, cr.read_hash, cr.write_size
				blob_path = get_blob_path_for_write(blob_hash)

				# reference: shutil.move, but os.replace is used
				try:
					with self.__time_costs.measure_time_cost(TimeCostKey.kind_fs):
						os.replace(temp_file_path, blob_path)
				except OSError:
					# The temp dir is in the different file system to the blob store?
					# Whatever, use file copy as the fallback
					# the temp file will be deleted automatically
					with self.__time_costs.measure_time_cost(TimeCostKey.kind_io_copy):
						file_utils.copy_file_fast(temp_file_path, blob_path)

		else:
			misc_utils.assert_true(blob_hash is not None, 'blob_hash is None')
			blob_path = get_blob_path_for_write(blob_hash)

			if policy == _DirectBlobCreatePolicy.read_all:
				# the file content is already in memory, just write+compress to blob store
				misc_utils.assert_true(blob_content is not None, 'blob_content is None')
				with self.__time_costs.measure_time_cost(TimeCostKey.kind_io_write):
					with compressor.open_compressed_bypassed(blob_path) as (writer, f):
						f.write(blob_content)
				raw_size, stored_size = len(blob_content), writer.get_write_len()
			elif policy == _DirectBlobCreatePolicy.default:
				if can_copy_on_write and compress_method == CompressMethod.plain:
					# fast copy, then calc size and hash to verify
					with self.__time_costs.measure_time_cost(TimeCostKey.kind_io_copy):
						file_utils.copy_file_fast(src_path, blob_path, open_r_func=SourceFileNotFoundWrapper.open_rb)
					with self.__time_costs.measure_time_cost(TimeCostKey.kind_io_read):
						actual_sah = hash_utils.calc_file_size_and_hash(blob_path)
					raw_size = stored_size = actual_sah.size
				else:
					# copy+compress+hash to blob store
					with self.__time_costs.measure_time_cost(TimeCostKey.kind_io_copy):
						cr = compressor.copy_compressed(src_path, blob_path, calc_hash=True, open_r_func=SourceFileNotFoundWrapper.open_rb)
					raw_size, stored_size = cr.read_size, cr.write_size
					actual_sah = SizeAndHash(cr.read_size, cr.read_hash)
				check_changes(actual_sah.size, actual_sah.hash)
			else:
				raise AssertionError('bad policy {!r}'.format(policy))

		misc_utils.assert_true(blob_hash is not None, f'blob_hash is None, policy {policy}')
		misc_utils.assert_true(raw_size is not None, f'raw_size is None, policy {policy}')
		misc_utils.assert_true(stored_size is not None, f'stored_size is None, policy {policy}')
		return self.__blob_recorder.create_blob(
			self.session,
			storage_method=BlobStorageMethod.direct.value,
			hash=blob_hash,
			compress=compress_method.name,
			raw_size=raw_size,
			stored_size=stored_size,
		)

	def __try_get_or_create_chunked_blob(self, src_path: Path, src_path_md5: str, st: os.stat_result, last_chance: bool) -> Generator[Any, Any, schema.Blob]:
		log_and_raise_blob_file_changed = functools.partial(self.__log_and_raise_blob_file_changed, last_chance=last_chance)

		if last_chance:
			policy = _ChunkedBlobCreatePolicy.copy_hash
		else:
			policy = _ChunkedBlobCreatePolicy.default

		with contextlib.ExitStack() as es:
			if policy == _ChunkedBlobCreatePolicy.default:
				# TODO: pre-cal cdc cut result?
				actual_path_to_read = src_path
			elif policy == _ChunkedBlobCreatePolicy.copy_hash:
				# copy to temp file, then do whatever processing
				temp_file_path = es.enter_context(self.__make_temp_file(src_path_md5))
				with self.__time_costs.measure_time_cost(TimeCostKey.kind_io_copy):  # copy fast, no bypass tricks, since it's a volatile file
					file_utils.copy_file_fast(src_path, temp_file_path, open_r_func=SourceFileNotFoundWrapper.open_rb)
				actual_path_to_read = temp_file_path
			else:
				raise AssertionError('bad policy {!r}'.format(policy))

			chunker = chunk_utils.FileChunker(actual_path_to_read, need_entire_file_hash=True)
			with self.__time_costs.measure_time_cost(TimeCostKey.kind_io_read):
				chunks = chunker.cut_all()
			blob_hash = chunker.get_entire_file_hash()
			blob_size = chunker.get_read_file_size()

			if (cache := self.__blob_by_hash_cache.get(blob_hash)) is not None:
				return cache
			yield BlobByHashFetcher.Req(blob_hash)
			if (cache := self.__blob_by_hash_cache.get(blob_hash)) is not None:
				return cache
			# notes: the following code cannot be interrupted (yield).
			# The blob is specifically generated by the generator
			# if any yield is done, ensure to check __blob_by_hash_cache again

			# large files that need to be chunked are not common, and they already contains quite a few chunks
			# so it's efficient enough to directly query for chunks from DB here

			process_start_time = time.time()
			with self.__time_costs.measure_time_cost(TimeCostKey.kind_db):
				known_db_chunks = self.session.get_chunks_by_hashes([chunk.hash for chunk in chunks])
			new_db_chunks: List[schema.Chunk] = []
			offset_to_chunk_hash: Dict[int, str] = {}
			blob_raw_size_sum = 0
			blob_stored_size_sum = 0
			with open(actual_path_to_read, 'rb') as src_file:
				offset = 0
				# TODO: multithreading
				for chunk in chunks:
					misc_utils.assert_true(offset == chunk.offset, f'offset mismatch {offset} {chunk.offset}')
					misc_utils.assert_true(chunk.length > 0, 'chunk with zero length')

					if (db_chunk := known_db_chunks[chunk.hash]) is None:
						with self.__time_costs.measure_time_cost(TimeCostKey.kind_io_read):
							chunk_buf = src_file.read(chunk.length)
						if len(chunk_buf) != chunk.length:
							log_and_raise_blob_file_changed('Blob size mismatch, fail to read {} byte at offset {}, actual read {}'.format(chunk.length, offset, len(chunk_buf)))
						new_chunk_hash = hash_utils.calc_bytes_hash(chunk_buf)
						if new_chunk_hash != chunk.hash:
							log_and_raise_blob_file_changed('Blob content mismatch, chunk at [{}, {}) has its hash changed, previous {}, current {}'.format(offset, offset + chunk.length, chunk.hash, new_chunk_hash))

						compress_method: CompressMethod = self.config.backup.get_compress_method_from_size(chunk.length)
						compressor = Compressor.create(compress_method)
						chunk_path = chunk_utils.get_chunk_path(chunk.hash)
						self.__blob_recorder.add_remove_file_rollbacker(chunk_path)

						with self.__time_costs.measure_time_cost(TimeCostKey.kind_io_write):
							with compressor.open_compressed_bypassed(chunk_path) as (writer, f):
								f.write(chunk_buf)

						db_chunk = self.session.create_chunk(
							hash=chunk.hash,
							compress=compress_method.name,
							raw_size=len(chunk_buf),
							stored_size=writer.get_write_len(),
						)
						new_db_chunks.append(db_chunk)
						known_db_chunks[db_chunk.hash] = db_chunk
					else:
						if src_file.seekable():
							src_file.seek(offset + chunk.length)
						else:
							n_read = len(src_file.read(chunk.length))
							if n_read != chunk.length:
								log_and_raise_blob_file_changed('Blob size mismatch, fail to read {} byte at offset {}, actual read {}'.format(chunk.length, offset, n_read))

					blob_raw_size_sum += db_chunk.raw_size
					blob_stored_size_sum += db_chunk.stored_size
					offset_to_chunk_hash[offset] = db_chunk.hash
					offset += chunk.length

				misc_utils.assert_true(blob_raw_size_sum == offset, f'blob_raw_size_sum {blob_raw_size_sum} should be equal to offset {offset}')
				misc_utils.assert_true(blob_raw_size_sum == blob_size, f'blob_raw_size_sum {blob_raw_size_sum} should be equal to blob_size {blob_size}')

				extra_buf = src_file.read(1)
				if len(extra_buf) > 0:
					log_and_raise_blob_file_changed('Blob size mismatch, actual size larger than expected size {}'.format(blob_size))

		self.logger.debug('Chunked large file {} in {:.2f}s, size {}/{}, chunk cnt: {} (+{})'.format(
			repr(src_path.as_posix()), time.time() - process_start_time,
			blob_stored_size_sum, blob_raw_size_sum, len(offset_to_chunk_hash), len(new_db_chunks),
		))
		if len(new_db_chunks) >= max(500, int(len(known_db_chunks) * 0.25)):
			# 500 chunks == ~150MiB
			self.logger.warning('Chunked a large file with lots of new chunks, please consider if it should really be in the CDC target patterns')
			self.logger.warning('File path: {} size {}, chunk cnt {}, new chunk cnt {} ({:.1f}%), new chunk size {}'.format(
				repr(src_path.as_posix()), ByteCount(blob_raw_size_sum).auto_str(), len(known_db_chunks),
				len(new_db_chunks), 100.0 * len(new_db_chunks) / len(known_db_chunks),
				ByteCount(sum(db_chunk.raw_size for db_chunk in new_db_chunks)).auto_str(),
			))
			self.logger.warning('You can safely ignore this warning if this is the first backup containing the file')

		for new_db_chunk in new_db_chunks:
			self.session.add(new_db_chunk)
		blob = self.__blob_recorder.create_blob(
			self.session,
			storage_method=BlobStorageMethod.chunked.value,
			hash=blob_hash,
			compress=CompressMethod.plain.name,
			raw_size=blob_raw_size_sum,
			stored_size=blob_stored_size_sum,
		)
		with self.__time_costs.measure_time_cost(TimeCostKey.kind_db):
			self.session.flush()  # creates blob.id, chunk.id

		chunk_hash_chunk = {db_chunk.hash: db_chunk for db_chunk in known_db_chunks.values()}
		ChunkGrouper(self.session).create_chunk_groups(blob, {
			offset: chunk_hash_chunk[chunk_hash]
			for offset, chunk_hash in offset_to_chunk_hash.items()
		})
		return blob

	@functools.cached_property
	def __skip_missing_source_file_patterns(self) -> pathspec.GitIgnoreSpec:
		return pathspec.GitIgnoreSpec.from_lines(self.config.backup.creation_skip_missing_file_patterns)

	@functools.cached_property
	def __cdc_patterns(self) -> pathspec.GitIgnoreSpec:
		return pathspec.GitIgnoreSpec.from_lines(self.config.backup.cdc_patterns)

	def __should_skip_missing_source_file(self, src_file_path: Path) -> bool:
		if self.config.backup.creation_skip_missing_file:
			try:
				rel_path = src_file_path.relative_to(self.__source_path)
			except ValueError:
				self.logger.error("Path {!r} is not inside the source path {!r}".format(str(src_file_path), str(self.__source_path)))
			else:
				return self.__skip_missing_source_file_patterns.match_file(rel_path)
		return False

	def __matches_cdc_pattern(self, file_path: Path) -> bool:
		try:
			rel_path = file_path.relative_to(self.__source_path)
		except ValueError:
			self.logger.error("Path {!r} is not inside the source path {!r}".format(str(file_path), str(self.__source_path)))
			return False
		else:
			return self.__cdc_patterns.match_file(rel_path)

	def __try_get_or_create_blob_once(self, src_path: Path, src_path_md5: str, st: os.stat_result, last_chance: bool) -> Generator[Any, Any, schema.Blob]:
		if self.config.backup.cdc_enabled and st.st_size >= self.config.backup.cdc_file_size_threshold and self.__matches_cdc_pattern(src_path):
			gen = self.__try_get_or_create_chunked_blob(src_path, src_path_md5, st, last_chance)
		else:
			gen = self.__try_get_or_create_direct_blob(src_path, src_path_md5, st, last_chance)
		try:
			query = gen.send(None)
			while True:
				result = yield query
				query = gen.send(result)
		except StopIteration as e:  # ok
			return e.value

	def get_or_create_blob(self, src_path: Path, st: os.stat_result) -> Generator[Any, Any, GetOrCreateBlobResult]:
		src_path_str = repr(src_path.as_posix())
		src_path_md5 = hashlib.md5(src_path_str.encode('utf8')).hexdigest()

		for i in range(_BLOB_FILE_CHANGED_RETRY_COUNT):
			retry_cnt = i + 1  # [1, n]
			is_last_attempt = retry_cnt == _BLOB_FILE_CHANGED_RETRY_COUNT
			if i > 0:
				self.logger.debug('Try to create blob {} (attempt {} / {})'.format(src_path_str, retry_cnt, _BLOB_FILE_CHANGED_RETRY_COUNT))
			gen = self.__try_get_or_create_blob_once(src_path, src_path_md5, st, last_chance=is_last_attempt)
			try:
				query = gen.send(None)
				while True:
					result = yield query
					query = gen.send(result)
			except StopIteration as e:  # ok
				blob: schema.Blob = e.value
				self.__update_blob_cache(blob)
				return GetOrCreateBlobResult(blob, st)
			except _BlobFileChanged:
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
		with self.__time_costs.measure_time_cost(TimeCostKey.stage_prepare_blob_store, TimeCostKey.kind_fs):
			blob_utils.prepare_blob_directories()
			chunk_utils.prepare_chunk_directories()

			bs_path = blob_utils.get_blob_store()
			self.__blob_store_st = bs_path.stat()
			self.__blob_store_in_cow_fs = file_utils.does_fs_support_cow(bs_path)

	def schedule_loop(self, gen_list: List[Generator[BqmReq, Optional[BqmRsp], schema.File]]) -> List[schema.File]:
		files: List[schema.File] = []

		schedule_queue: Deque[Tuple[Generator[BqmReq, Optional[BqmRsp], schema.File], Optional[BqmRsp]]] = collections.deque()
		for gen in gen_list:
			schedule_queue.append((gen, None))

		while len(schedule_queue) > 0:
			gen, value = schedule_queue.popleft()
			try:
				def callback(query_rsp: BqmRsp, g=gen):
					schedule_queue.appendleft((g, query_rsp))

				query_req = gen.send(value)
				self.__batch_query_manager.query(query_req, callback)
			except StopIteration as e:
				files.append(misc_utils.ensure_type(e.value, schema.File))
			except SourceFileNotFoundWrapper as e:
				if self.__should_skip_missing_source_file(e.file_path):
					self.logger.warning('Backup source file {!r} not found, suppressed and skipped by config'.format(str(e.file_path)))
				else:
					raise

			self.__batch_query_manager.flush_if_needed()
			if len(schedule_queue) == 0:
				self.__batch_query_manager.flush()

		return files

	def __update_blob_cache(self, blob: schema.Blob):
		if blob is not None:
			self.__blob_by_size_cache[blob.raw_size] = True
			self.__blob_by_hash_cache[blob.hash] = blob

	def add_existing_sizes(self, existing_sizes: Dict[int, bool]):
		self.__blob_by_size_cache.update(existing_sizes)
