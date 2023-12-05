import collections
import contextlib
import enum
import functools
import hashlib
import os
import stat
import threading
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Tuple, Callable, Any, Dict, NamedTuple, Generator, Union, Set, Deque, ContextManager

from prime_backup.action.create_backup_action_base import CreateBackupActionBase
from prime_backup.compressors import Compressor, CompressMethod
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.exceptions import PrimeBackupError
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.backup_tags import BackupTags
from prime_backup.types.operator import Operator
from prime_backup.types.units import ByteCount
from prime_backup.utils import hash_utils, misc_utils, blob_utils, file_utils


class VolatileBlobFile(PrimeBackupError):
	pass


class _BlobFileChanged(PrimeBackupError):
	pass


class _BlobCreatePolicy(enum.Enum):
	"""
	the policy of how to create a blob from a given file path
	"""
	read_all = enum.auto()   # small files: read all in memory, calc hash. read once
	hash_once = enum.auto()  # files with unique size: compress+hash to temp file, then move. read once
	copy_hash = enum.auto()  # files that keep changing: copy to temp file, calc hash, compress to blob. read twice and need more spaces
	default = enum.auto()    # default policy: compress+hash to blob store, check hash again. read twice


_BLOB_FILE_CHANGED_RETRY_COUNT = 3
_READ_ALL_SIZE_THRESHOLD = 8 * 1024  # 8KiB
_HASH_ONCE_SIZE_THRESHOLD = 10 * 1024 * 1024  # 10MiB


class BatchFetcherBase(ABC):
	Callback = Callable
	tasks: dict

	def __init__(self, session: DbSession, max_batch_size: int):
		self.session = session
		self.max_batch_size = max_batch_size
		self.first_task_scheduled_time = time.time()

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
	class Req(NamedTuple):
		size: int

	class Rsp(NamedTuple):
		exists: bool

	Callback = Callable[[Rsp], Any]
	tasks: Dict[int, List[Callback]]

	def __init__(self, session: DbSession, max_batch_size: int, result_cache: Dict[int, bool]):
		super().__init__(session, max_batch_size)
		self.tasks: List[Tuple[int, BlobBySizeFetcher.Callback]] = []
		self.sizes: Set[int] = set()
		self.result_cache = result_cache

	def query(self, query: Req, callback: Callback):
		self.tasks.append((query.size, callback))
		self.sizes.add(query.size)
		self._post_query()

	def _batch_run(self):
		existence = self.session.has_blob_with_size_batched(list(self.sizes))
		for sz, e in existence.items():
			self.result_cache[sz] = e
		# reverse since we want to keep the file order, and collections.deque.appendleft is FILO
		for sz, callback in reversed(self.tasks):
			callback(self.Rsp(existence[sz]))
		self.tasks.clear()
		self.sizes.clear()


class BlobByHashFetcher(BatchFetcherBase):
	class Req(NamedTuple):
		hash: str

	class Rsp(NamedTuple):
		blob: Optional[schema.Blob]

	Callback = Callable[[Rsp], Any]
	tasks: Dict[str, List[Callback]]

	def __init__(self, session: DbSession, max_batch_size: int, result_cache: Dict[str, schema.Blob]):
		super().__init__(session, max_batch_size)
		self.tasks: List[Tuple[str, BlobByHashFetcher.Callback]] = []
		self.hashes: Set[str] = set()
		self.result_cache = result_cache

	def query(self, query: Req, callback: Callback):
		self.tasks.append((query.hash, callback))
		self.hashes.add(query.hash)
		self._post_query()

	def _batch_run(self):
		blobs = self.session.get_blobs(list(self.hashes))
		for h, blob in blobs.items():
			self.result_cache[h] = blob
		# reverse since we want to keep the file order, and collections.deque.appendleft is FILO
		for h, callback in reversed(self.tasks):
			callback(self.Rsp(blobs[h]))
		self.tasks.clear()
		self.hashes.clear()


class BatchQueryManager:
	Reqs = Union[BlobBySizeFetcher.Req, BlobByHashFetcher.Req]

	def __init__(self, session: DbSession, size_result_cache: dict, hash_result_cache: dict, max_batch_size: int = 100):
		self.fetcher_size = BlobBySizeFetcher(session, max_batch_size, size_result_cache)
		self.fetcher_hash = BlobByHashFetcher(session, max_batch_size, hash_result_cache)

	def query(self, query: Reqs, callback: callable):
		if isinstance(query, BlobBySizeFetcher.Req):
			self.fetcher_size.query(query, callback)
		elif isinstance(query, BlobByHashFetcher.Req):
			self.fetcher_hash.query(query, callback)
		else:
			raise ValueError('unexpected query: {!r} {!r}'.format(type(query), query))

	def flush_if_needed(self):
		self.fetcher_size.flush_if_needed()
		self.fetcher_hash.flush_if_needed()

	def flush(self):
		self.fetcher_size.flush()
		self.fetcher_hash.flush()


class CreateBackupAction(CreateBackupActionBase):
	def __init__(self, author: Operator, comment: str, *, tags: Optional[BackupTags] = None, expire_timestamp_ns: Optional[int] = None):
		super().__init__()
		if tags is None:
			tags = BackupTags()

		self.author = author
		self.comment = comment
		self.tags = tags
		self.expire_timestamp_ns = expire_timestamp_ns

		self.__blob_store_st: Optional[os.stat_result] = None
		self.__blob_store_in_cow_fs: Optional[bool] = None

		self.__batch_query_manager: Optional[BatchQueryManager] = None
		self.__blob_by_size_cache: Dict[int, bool] = {}
		self.__blob_by_hash_cache: Dict[str, schema.Blob] = {}

	def scan_files(self) -> List[Path]:
		collected = []

		for target in self.config.backup.targets:
			target_path = self.config.source_path / target
			if not target_path.exists():
				self.logger.info('skipping not-exist backup target {}'.format(target_path))
				continue

			collected.append(target_path)
			if target_path.is_dir():
				for dir_path, dir_names, file_names in os.walk(target_path):
					for name in file_names + dir_names:
						collected.append(Path(dir_path) / name)

		return [p for p in collected if not self.config.backup.is_file_ignore(p)]

	def __get_or_create_blob(self, session: DbSession, src_path: Path, st: os.stat_result) -> Generator[Any, Any, Tuple[schema.Blob, os.stat_result]]:
		src_path_str = repr(src_path.as_posix())
		src_path_md5 = hashlib.md5(src_path_str.encode('utf8')).hexdigest()

		@contextlib.contextmanager
		def make_temp_file() -> ContextManager[Path]:
			temp_id = f'blob-{threading.current_thread().ident}-{src_path_md5}'
			temp_file_path = self.config.storage_path / 'temp' / '{}.tmp'.format(temp_id)
			temp_file_path.parent.mkdir(parents=True, exist_ok=True)
			with contextlib.ExitStack() as exit_stack:
				exit_stack.callback(functools.partial(self._remove_file, temp_file_path))
				yield temp_file_path

		def attempt_once(last_chance: bool = False) -> Generator[Any, Any, schema.Blob]:
			compress_method = self.config.backup.get_compress_method_from_size(st.st_size)
			can_copy_on_write = (
					file_utils.HAS_COPY_FILE_RANGE and
					compress_method == CompressMethod.plain and
					self.__blob_store_in_cow_fs and
					st.st_dev == self.__blob_store_st.st_dev
			)

			policy: Optional[_BlobCreatePolicy] = None
			blob_hash: Optional[str] = None
			blob_content: Optional[bytes] = None
			raw_size: Optional[int] = None
			stored_size: Optional[int] = None

			if last_chance:
				policy = _BlobCreatePolicy.copy_hash
			elif st.st_size <= _READ_ALL_SIZE_THRESHOLD:
				policy = _BlobCreatePolicy.read_all
				with open(src_path, 'rb') as f:
					blob_content = f.read(_READ_ALL_SIZE_THRESHOLD + 1)
				if len(blob_content) > _READ_ALL_SIZE_THRESHOLD:
					self.logger.warning('Read too many bytes for read_all policy, stat: {}, read: {}'.format(st.st_size, len(blob_content)))
					raise _BlobFileChanged()
				blob_hash = hash_utils.calc_bytes_hash(blob_content)
			elif not can_copy_on_write and st.st_size > _HASH_ONCE_SIZE_THRESHOLD:
				can_hash_once = self.__blob_by_size_cache.get(st.st_size, False) is False
				if can_hash_once:
					# noinspection PyTypeChecker
					yield BlobBySizeFetcher.Req(st.st_size)
					can_hash_once = self.__blob_by_size_cache.get(st.st_size, False) is False
				if can_hash_once:
					# it's certain that this blob is unique, but notes: the following code
					# cannot be interrupted (yield), or other generator could make a same blob
					policy = _BlobCreatePolicy.hash_once
			if policy is None:
				policy = _BlobCreatePolicy.default
				blob_hash = hash_utils.calc_file_hash(src_path)

			# self.logger.info("%s %s %s", policy.name, compress_method.name, src_path)
			if blob_hash is not None:
				misc_utils.assert_true(policy != _BlobCreatePolicy.hash_once, 'unexpected policy')

				if (cache := self.__blob_by_hash_cache.get(blob_hash)) is not None:
					return cache
				yield BlobByHashFetcher.Req(blob_hash)
				if (cache := self.__blob_by_hash_cache.get(blob_hash)) is not None:
					return cache

			# notes: the following code cannot be interrupted (yield).
			# The blob is specifically generated by the generator
			# if any yield is done, ensure to check __blob_by_hash_cache again

			def check_changes(new_size: int, new_hash: Optional[str]):
				if new_size != st.st_size:
					self.logger.warning('Blob size mismatch, previous: {}, current: {}'.format(st.st_size, new_size))
					raise _BlobFileChanged()
				if blob_hash is not None and new_hash is not None and new_hash != blob_hash:
					self.logger.warning('Blob hash mismatch, previous: {}, current: {}'.format(blob_hash, new_hash))
					raise _BlobFileChanged()

			def bp_rba(h: str) -> Path:
				bp = blob_utils.get_blob_path(h)
				self._add_remove_file_rollbacker(bp)
				return bp

			compressor = Compressor.create(compress_method)
			if policy == _BlobCreatePolicy.copy_hash:
				# copy to temp file, calc hash, then compress to blob store
				misc_utils.assert_true(blob_hash is None, 'blob_hash should not be calculated')
				with make_temp_file() as temp_file_path:
					file_utils.copy_file_fast(src_path, temp_file_path)
					blob_hash = hash_utils.calc_file_hash(temp_file_path)

					misc_utils.assert_true(last_chance, 'only last_chance=True can use do hash_once without checking uniqueness')
					if (cache := self.__blob_by_hash_cache.get(blob_hash)) is not None:
						return cache
					yield BlobByHashFetcher.Req(blob_hash)
					if (cache := self.__blob_by_hash_cache.get(blob_hash)) is not None:
						return cache

					blob_path = bp_rba(blob_hash)
					cr = compressor.copy_compressed(temp_file_path, blob_path, calc_hash=False)
					raw_size, stored_size = cr.read_size, cr.write_size

			elif policy == _BlobCreatePolicy.hash_once:
				# read once, compress+hash to temp file, then move
				misc_utils.assert_true(blob_hash is None, 'blob_hash should not be calculated')
				with make_temp_file() as temp_file_path:
					cr = compressor.copy_compressed(src_path, temp_file_path, calc_hash=True)
					check_changes(cr.read_size, None)  # the size must be unchanged, to satisfy the uniqueness

					raw_size, blob_hash, stored_size = cr.read_size, cr.read_hash, cr.write_size
					blob_path = bp_rba(blob_hash)
					os.rename(temp_file_path, blob_path)

			else:
				misc_utils.assert_true(blob_hash is not None, 'blob_hash is None')
				blob_path = bp_rba(blob_hash)

				if policy == _BlobCreatePolicy.read_all:
					# the file content is already in memory, just write+compress to blob store
					misc_utils.assert_true(blob_content is not None, 'blob_content is None')
					with compressor.open_compressed_bypassed(blob_path) as (writer, f):
						f.write(blob_content)
					raw_size, stored_size = len(blob_content), writer.get_write_len()
				elif policy == _BlobCreatePolicy.default:
					if can_copy_on_write and compress_method == CompressMethod.plain:
						# fast copy, then calc size and hash to verify
						file_utils.copy_file_fast(src_path, blob_path)
						stored_size, h2 = hash_utils.calc_file_size_and_hash(blob_path)
						raw_size = stored_size
						check_changes(stored_size, h2)
					else:
						# copy+compress+hash to blob store
						cr = compressor.copy_compressed(src_path, blob_path, calc_hash=True)
						raw_size, stored_size = cr.read_size, cr.write_size
						check_changes(cr.read_size, cr.read_hash)
				else:
					raise AssertionError()

			misc_utils.assert_true(blob_hash is not None, 'blob_hash is None')
			misc_utils.assert_true(raw_size is not None, 'stored_size is None')
			misc_utils.assert_true(stored_size is not None, 'stored_size is None')
			return self._create_blob(
				session,
				hash=blob_hash,
				compress=compress_method.name,
				raw_size=raw_size,
				stored_size=stored_size,
			)

		for i in range(_BLOB_FILE_CHANGED_RETRY_COUNT):
			last_attempt = i == _BLOB_FILE_CHANGED_RETRY_COUNT - 1
			if i > 0:
				self.logger.warning('Try to create blob {} (attempt {} / {})'.format(src_path_str, i + 1, _BLOB_FILE_CHANGED_RETRY_COUNT))
			gen = attempt_once(last_chance=last_attempt)
			try:
				query = gen.send(None)
				while True:
					result = yield query
					query = gen.send(result)
			except StopIteration as e:  # ok
				blob: schema.Blob = e.value
				self.__blob_by_size_cache[blob.raw_size] = True
				self.__blob_by_hash_cache[blob.hash] = blob
				return blob, st
			except _BlobFileChanged:
				self.logger.warning('Blob {} stat has changed, {}'.format(src_path_str, 'no more retry' if last_attempt else 'retrying'))
				st = src_path.stat()

		self.logger.error('All blob copy attempts failed, is the file {} keeps changing?'.format(src_path_str))
		raise VolatileBlobFile('blob file {} keeps changing'.format(src_path_str))

	def __create_file(self, session: DbSession, path: Path) -> Generator[Any, Any, schema.File]:
		related_path = path.relative_to(self.config.source_path)
		st = path.stat()

		blob: Optional[schema.Blob] = None
		content: Optional[bytes] = None
		if stat.S_ISREG(st.st_mode):
			gen = self.__get_or_create_blob(session, path, st)
			try:
				query = gen.send(None)
				while True:
					result = yield query
					query = gen.send(result)
			except StopIteration as e:
				blob, st = e.value
				# notes: st.st_size might be incorrect, use blob.raw_size instead
		elif stat.S_ISDIR(st.st_mode):
			pass
		elif stat.S_ISLNK(st.st_mode):
			content = path.readlink().as_posix().encode('utf8')
		else:
			raise NotImplementedError('unsupported yet')

		return session.create_file(
			path=related_path.as_posix(),
			content=content,

			mode=st.st_mode,
			uid=st.st_uid,
			gid=st.st_gid,
			ctime_ns=st.st_ctime_ns,
			mtime_ns=st.st_mtime_ns,
			atime_ns=st.st_atime_ns,

			add_to_session=False,
			blob=blob,
		)

	def run(self) -> BackupInfo:
		super().run()
		self.__blob_by_size_cache.clear()
		self.__blob_by_hash_cache.clear()

		try:
			with DbAccess.open_session() as session:
				self.__batch_query_manager = BatchQueryManager(session, self.__blob_by_size_cache, self.__blob_by_hash_cache)

				backup = session.create_backup(
					author=str(self.author),
					comment=self.comment,
					targets=[Path(t).as_posix() for t in self.config.backup.targets],
					tags=self.tags.to_dict()
				)
				self.logger.info('Creating backup {}'.format(backup))

				blob_utils.prepare_blob_directories()
				bs_path = blob_utils.get_blob_store()
				self.__blob_store_st = bs_path.stat()
				self.__blob_store_in_cow_fs = file_utils.does_fs_support_cow(bs_path)

				files = []
				schedule_queue: Deque[Tuple[Generator, Any]] = collections.deque()
				for file_path in self.scan_files():
					schedule_queue.append((self.__create_file(session, file_path), None))
				while len(schedule_queue) > 0:
					gen, value = schedule_queue.popleft()
					try:
						def callback(v, g=gen):
							schedule_queue.appendleft((g, v))

						query = gen.send(value)
						self.__batch_query_manager.query(query, callback)
					except StopIteration as e:
						files.append(misc_utils.ensure_type(e.value, schema.File))

					self.__batch_query_manager.flush_if_needed()
					if len(schedule_queue) == 0:
						self.__batch_query_manager.flush()

				session.flush()  # generate backup id
				for file in files:
					file.backup_id = backup.id
					session.add(file)

				info = BackupInfo.of(backup)

			s = self.get_new_blobs_summary()
			self.logger.info('Create backup #{} done, +{} blobs (size {} / {})'.format(
				info.id, s.count, ByteCount(s.stored_size).auto_str(), ByteCount(s.raw_size).auto_str(),
			))
			return info

		except Exception as e:
			self._apply_blob_rollback()
			raise e
