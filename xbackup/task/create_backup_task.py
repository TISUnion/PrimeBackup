import contextlib
import enum
import functools
import os
import stat
import threading
import time
from pathlib import Path
from typing import List, Optional, Tuple, Callable, Any, Dict, NamedTuple, Generator

import psutil

from xbackup import logger
from xbackup.compressors import Compressor, CompressMethod
from xbackup.config.config import Config
from xbackup.config.sub_configs import BackupJob
from xbackup.db import schema
from xbackup.db.access import DbAccess
from xbackup.db.session import DbSession
from xbackup.task.task import Task
from xbackup.utils import hash_utils, misc_utils, blob_utils, file_utils


class VolatileBlobFile(Exception):
	pass


class _BlobFileChanged(Exception):
	pass


class _BlobCreatePolicy(enum.Enum):
	"""
	the policy of how to create a blob from a given file path
	"""
	read_all = enum.auto()  # for small files: read all in memory, calc hash. read once
	hash_once = enum.auto()  # files with unique size: compress+hash to temp file, then move. read once
	default = enum.auto()  # file with duplicated size: read twice (slower)


_BLOB_FILE_CHANGED_RETRY_COUNT = 3
_HASH_ONCE_SIZE_THRESHOLD = 100 * 1024 * 1024  # 100MiB


class QueryBlobHashReq(NamedTuple):
	hash: str


class QueryBlobHashRsp(NamedTuple):
	blob: Optional[schema.Blob]


class BatchBlobFetcher:
	Callback = Callable[[Optional[schema.Blob]], Any]

	def __init__(self, session: DbSession, max_batch_size: int = 100):
		self.session = session
		self.max_batch_size = max_batch_size
		self.tasks: Dict[str, BatchBlobFetcher.Callback] = {}
		self.first_task_scheduled_time = time.time()

	def get_by_hash(self, h: str, callback: Callback):
		now = time.time()
		if len(self.tasks) == 0:
			self.first_task_scheduled_time = now

		self.tasks[h] = callback

		# too much tasks, or too much time elapsed (0.1s) since the 1st task
		if len(self.tasks) >= self.max_batch_size or now - self.first_task_scheduled_time >= 0.1:
			self.__batch_run()

	def flush(self):
		if len(self.tasks) > 0:
			self.__batch_run()

	def __batch_run(self):
		# print(time.time() - self.first_task_scheduled_time, len(self.tasks))
		blobs = self.session.get_blobs(list(self.tasks.keys()))
		for h, callback in self.tasks.items():
			callback(blobs.get(h))
		self.tasks.clear()


def _calc_if_is_fast_copy_fs(path: Path) -> bool:
	path = path.absolute()
	mount_point: Optional[str] = None
	fs_type = '?'
	for p in psutil.disk_partitions():
		if p.mountpoint and p.fstype and path.is_relative_to(p.mountpoint):
			if mount_point is None or Path(p.mountpoint).is_relative_to(mount_point):
				mount_point = p.mountpoint
				fs_type = p.fstype.lower()
	logger.get().debug(f'fs for {path}: {fs_type} {mount_point}')
	return fs_type in ['xfs', 'zfs', 'btrfs', 'apfs', 'refs']


class CreateBackupTask(Task):
	def __init__(self, job: BackupJob, author: str, comment: str):
		super().__init__()
		self.job = job
		self.author = author
		self.comment = comment

		self.config = Config.get()
		self.__backup_id: Optional[int] = None
		self.__blob_cache: Dict[str, schema.Blob] = {}
		self.__blobs_rollbackers: List[callable] = []
		self.__blob_store_st: Optional[os.stat_result] = None
		self.__blob_store_in_fast_copy_fs: Optional[bool] = None

		self.__batch_blob_fetcher: Optional[BatchBlobFetcher] = None

	@property
	def backup_id(self) -> int:
		if self.__backup_id is None:
			raise ValueError('backup is not created yet')
		return self.__backup_id

	def scan_files(self) -> List[Path]:
		collected = []

		for target in self.job.targets:
			target_path = self.job.source_path / target
			if not target_path.exists():
				self.logger.info('skipping not-exist backup target {}'.format(target_path))
				continue

			if target_path.is_dir():
				for dir_path, dir_names, file_names in os.walk(target_path):
					for name in file_names + dir_names:
						collected.append(Path(dir_path) / name)
			else:
				collected.append(target_path)

		return [p for p in collected if not self.job.is_file_ignore(p)]

	def __remove_file(self, file_to_remove: Path):
		try:
			if file_to_remove.is_file():
				file_to_remove.unlink(missing_ok=True)
		except OSError as e:
			self.logger.error('(rollback) remove file {!r} failed: {}'.format(file_to_remove, e))

	def __get_or_create_blob(self, session: DbSession, src_path: Path, st: os.stat_result) -> Generator[Any, Any, Tuple[schema.Blob, os.stat_result]]:
		def attempt_once() -> Generator[Any, Any, schema.Blob]:
			if st.st_size < self.config.backup.compress_threshold:
				compress_method = CompressMethod.plain
			else:
				compress_method = self.config.backup.compress_method

			can_fast_copy = (
					file_utils.HAS_COPY_FILE_RANGE and
					self.__blob_store_in_fast_copy_fs and
					compress_method == CompressMethod.plain and
					st.st_dev == self.__blob_store_st.st_dev
			)
			read_all_size_threshold = 4 * 1024 if can_fast_copy else 64 * 1024

			blob_content: Optional[bytes] = None
			if st.st_size < read_all_size_threshold:
				policy = _BlobCreatePolicy.read_all
				with open(src_path, 'rb') as f:
					blob_content = f.read(read_all_size_threshold + 1)
				if len(blob_content) != st.st_size:
					self.logger.warning('File size mismatch, stat: {}, read: {}'.format(st.st_size, len(blob_content)))
					raise _BlobFileChanged()
				blob_hash = hash_utils.calc_bytes_hash(blob_content)
			elif not can_fast_copy and st.st_size > _HASH_ONCE_SIZE_THRESHOLD and not session.has_blob_with_size(st.st_size):
				policy = _BlobCreatePolicy.hash_once
				blob_hash = None
			else:
				policy = _BlobCreatePolicy.default
				blob_hash = hash_utils.calc_file_hash(src_path)

			if blob_hash is not None:
				cache = self.__blob_cache.get(blob_hash)
				if cache is not None:
					return cache

				existing = yield QueryBlobHashReq(blob_hash)
				if existing.blob is not None:
					return existing.blob

				# other generators might just create the blob
				cache = self.__blob_cache.get(blob_hash)
				if cache is not None:
					return cache

			def check_changes(new_size: int, new_hash: Optional[str]):
				if new_size != st.st_size:
					self.logger.warning('Blob size mismatch, previous: {}, current: {}'.format(st.st_size, new_size))
					raise _BlobFileChanged()
				if blob_hash is not None and new_hash is not None and new_hash != blob_hash:
					self.logger.warning('Blob hash mismatch, previous: {}, current: {}'.format(blob_hash, new_hash))
					raise _BlobFileChanged()

			def bp_rba(h: str) -> Path:
				bp = blob_utils.get_blob_path(h)
				self.__blobs_rollbackers.append(functools.partial(self.__remove_file, bp))
				return bp

			compressor = Compressor.create(compress_method)
			if policy == _BlobCreatePolicy.hash_once:
				# read once, compress+hash to temp file, then move
				temp_file_path = self.config.storage_path / 'temp' / '{}.tmp'.format(threading.current_thread().ident or 'backup')
				temp_file_path.parent.mkdir(parents=True, exist_ok=True)

				with contextlib.ExitStack() as exit_stack:
					exit_stack.callback(functools.partial(self.__remove_file, temp_file_path))

					cp_size, blob_hash = compressor.copy_compressed(src_path, temp_file_path, calc_hash=True)
					check_changes(cp_size, None)

					blob_path = bp_rba(blob_hash)
					os.rename(temp_file_path, blob_path)
			else:  # hash already calculated
				misc_utils.assert_true(blob_hash is not None, 'blob_hash is None')
				blob_path = bp_rba(blob_hash)

				if policy == _BlobCreatePolicy.read_all:
					# the file content is already in memory, no need to read
					misc_utils.assert_true(blob_content is not None, 'blob_content is None')
					with compressor.open_compressed(blob_path) as f:
						f.write(blob_content)
				elif policy == _BlobCreatePolicy.default:
					if can_fast_copy and compress_method == CompressMethod.plain:
						# fast copy + hash again might be faster than simple copy+hash
						file_utils.copy_file_fast(src_path, blob_path)
						check_changes(*hash_utils.calc_file_size_and_hash(blob_path))
					else:
						cr = compressor.copy_compressed(src_path, blob_path, calc_hash=True)
						check_changes(cr.size, cr.hash)
				else:
					raise AssertionError()

			misc_utils.assert_true(blob_hash is not None, 'blob_hash is None')
			blob = session.create_blob(hash=blob_hash, compress=compress_method.name, size=st.st_size)
			# self.logger.info('created %s %s', src_path, blob)
			self.__blob_cache[blob_hash] = blob
			return blob

		for i in range(_BLOB_FILE_CHANGED_RETRY_COUNT):
			gen = attempt_once()
			try:
				query = gen.send(None)
				while True:
					result = yield query
					query = gen.send(result)
			except StopIteration as e:  # ok
				return e.value, st
			except _BlobFileChanged:
				self.logger.warning('Blob {} stat has changed, retrying (attempt {} / {})'.format(src_path, i + 1, _BLOB_FILE_CHANGED_RETRY_COUNT))
				st = src_path.stat()

		self.logger.error('All blob copy attempts failed, is the file {} keeps changing?'.format(src_path))
		raise VolatileBlobFile('blob file {} keeps changing'.format(src_path))

	def __create_file(self, session: DbSession, backup: schema.Backup, path: Path) -> Generator[Any, Any, schema.File]:
		related_path = path.relative_to(self.job.source_path)
		st = path.stat()

		blob, content = None, None
		if stat.S_ISDIR(st.st_mode):
			pass
		elif stat.S_ISREG(st.st_mode):
			gen = self.__get_or_create_blob(session, path, st)
			try:
				query = gen.send(None)
				while True:
					result = yield query
					query = gen.send(result)
			except StopIteration as e:
				blob, st = e.value
				# notes: st.st_size might be incorrect, use blob.size instead
		elif stat.S_ISLNK(st.st_mode):
			content = str(path.readlink().as_posix()).encode('utf8')
		else:
			raise NotImplementedError('unsupported yet')

		kwargs = dict(
			backup_id=backup.id,
			path=str(related_path.as_posix()),
			content=content,

			mode=st.st_mode,
			uid=st.st_uid,
			gid=st.st_gid,
			ctime_ns=st.st_ctime_ns,
			mtime_ns=st.st_mtime_ns,
			atime_ns=st.st_atime_ns,
		)
		if blob is not None:
			kwargs |= dict(
				blob_hash=blob.hash,
				blob_compress=blob.compress,
				blob_size=blob.size,
			)
		return session.create_file(**kwargs)

	def run(self):
		self.__blobs_rollbackers.clear()
		try:
			with DbAccess.open_session() as session:
				self.__batch_blob_fetcher = BatchBlobFetcher(session)

				backup = session.create_backup(
					author=self.author,
					comment=self.comment,
					targets=[str(Path(t).as_posix()) for t in self.job.targets],
				)
				self.logger.info('Creating backup {}'.format(backup))

				blob_utils.prepare_blob_directories()
				bs_path = blob_utils.get_blob_store()
				self.__blob_store_st = bs_path.stat()
				self.__blob_store_in_fast_copy_fs = _calc_if_is_fast_copy_fs(bs_path)
				self.logger.info('fast copy fs: %s', self.__blob_store_in_fast_copy_fs)

				# TODO: multithread support
				for p in self.scan_files():
					gen = self.__create_file(session, backup, p)
					with contextlib.suppress(StopIteration):
						query = gen.send(None)

						if isinstance(query, QueryBlobHashReq):
							def callback(b: Optional[schema.Blob], g=gen):
								with contextlib.suppress(StopIteration):
									g.send(QueryBlobHashRsp(b))
							self.__batch_blob_fetcher.get_by_hash(query.hash, callback)

				self.__batch_blob_fetcher.flush()

				self.logger.info('Create backup done, backup id {}'.format(backup.id))
				self.__backup_id = backup.id
		except Exception as e:
			self.logger.info('Error occurs, applying rollback')
			self.__backup_id = None
			for rollback_func in self.__blobs_rollbackers:
				rollback_func()
			raise e
