import contextlib
import enum
import functools
import os
import shutil
import stat
import threading
from pathlib import Path
from typing import List, Optional, Tuple

from xbackup import utils
from xbackup.compressors import Compressor, CompressMethod
from xbackup.config.config import Config
from xbackup.db import schema
from xbackup.db.access import DbAccess
from xbackup.db.session import DbSession
from xbackup.task.task import Task


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
_READ_ALL_SIZE_THRESHOLD = 256 * 1024  # 256KiB
_HASH_ONCE_SIZE_THRESHOLD = 100 * 1024 * 1024  # 100MiB


class CreateBackupTask(Task):
	def __init__(self, author: str, comment: str):
		super().__init__()
		self.author = author
		self.comment = comment
		self.__blobs_rollbackers: List[callable] = []
		self.__backup_id: Optional[int] = None
		self.config = Config.get()

	@property
	def backup_id(self) -> int:
		if self.__backup_id is None:
			raise ValueError('backup is not created yet')
		return self.__backup_id

	def scan_files(self) -> List[Path]:
		collected = []

		for target in self.config.backup.targets:
			target_path = self.config.source_path / target
			if not target_path.exists():
				self.logger.info('skipping not-exist backup target {}'.format(target_path))
				continue

			if target_path.is_dir():
				for dir_path, dir_names, file_names in os.walk(target_path):
					for name in file_names + dir_names:
						collected.append(Path(dir_path) / name)
			else:
				collected.append(target_path)

		return [p for p in collected if not self.config.backup.is_file_ignore(p)]

	def __remove_file(self, file_to_remove: Path):
		try:
			if file_to_remove.is_file():
				file_to_remove.unlink(missing_ok=True)
		except OSError as e:
			self.logger.error('(rollback) remove file {!r} failed: {}'.format(file_to_remove, e))

	def __get_or_create_blob(self, session: DbSession, path: Path, st: os.stat_result) -> Tuple[schema.Blob, os.stat_result]:
		def attempt_once() -> schema.Blob:
			blob_content: Optional[bytes] = None
			if st.st_size < _READ_ALL_SIZE_THRESHOLD:
				policy = _BlobCreatePolicy.read_all
				with open(path, 'rb') as f:
					blob_content = f.read(_READ_ALL_SIZE_THRESHOLD + 1)
				if len(blob_content) != st.st_size:
					self.logger.warning('File size mismatch, stat: {}, read: {}'.format(st.st_size, len(blob_content)))
					raise _BlobFileChanged()
				blob_hash = utils.calc_bytes_hash(blob_content)
			elif st.st_size > _HASH_ONCE_SIZE_THRESHOLD and not session.has_blob_with_size(st.st_size):
				policy = _BlobCreatePolicy.hash_once
				blob_hash = None
			else:
				policy = _BlobCreatePolicy.default
				blob_hash = utils.calc_file_hash(path)

			if blob_hash is not None:
				existing = session.get_blob(blob_hash)
				if existing is not None:
					return existing

			if st.st_size < Config.get().backup.compress_threshold:
				method = CompressMethod.plain
			else:
				method = Config.get().backup.compress_method
			compressor = Compressor.create(method)

			def check_changes(new_size: int, new_hash: Optional[str] = None):
				if new_size != st.st_size:
					self.logger.warning('File size mismatch, previous: {}, current: {}'.format(st.st_size, new_size))
					raise _BlobFileChanged()
				if blob_hash is not None and new_hash is not None and new_hash != blob_hash:
					self.logger.warning('File size mismatch, previous: {}, current: {}'.format(blob_hash, new_hash))
					raise _BlobFileChanged()

			if policy == _BlobCreatePolicy.hash_once:
				# read once, compress+hash to temp file, then move
				temp_file_path = Config.get().storage_path / 'temp' / '{}.tmp'.format(threading.current_thread().ident or 'backup')
				temp_file_path.parent.mkdir(parents=True, exist_ok=True)

				with contextlib.ExitStack() as exit_stack:
					exit_stack.callback(functools.partial(self.__remove_file, temp_file_path))

					cp_size, blob_hash = compressor.copy_compressed(path, temp_file_path, calc_hash=True)
					check_changes(cp_size, None)

					blob_path = utils.get_blob_path(blob_hash)
					self.__blobs_rollbackers.append(functools.partial(self.__remove_file, blob_path))

					shutil.move(temp_file_path, blob_path)
			else:  # hash already calculated
				utils.assert_true(blob_hash is not None, 'blob_hash is None')
				blob_path = utils.get_blob_path(blob_hash)
				self.__blobs_rollbackers.append(functools.partial(self.__remove_file, blob_path))

				if policy == _BlobCreatePolicy.read_all:
					# the file content is already in memory, no need to read
					utils.assert_true(blob_content is not None, 'blob_content is None')
					with compressor.open_compressed(blob_path) as f:
						f.write(blob_content)
				else:
					utils.assert_true(policy == _BlobCreatePolicy.default, 'bad policy')
					cr = compressor.copy_compressed(path, blob_path, calc_hash=True)
					check_changes(cr.size, cr.hash)

			utils.assert_true(blob_hash is not None, 'blob_hash is None')
			return session.create_blob(hash=blob_hash, compress=method.name, size=st.st_size)

		for i in range(_BLOB_FILE_CHANGED_RETRY_COUNT):
			try:
				return attempt_once(), st
			except _BlobFileChanged:
				self.logger.warning('Blob file {} stat changes, retry. Attempt {} / {}'.format(path, i + 1, _BLOB_FILE_CHANGED_RETRY_COUNT))
				st = path.stat()

		self.logger.error('All blob copy attempts failed, is the file {} keeps changing?'.format(path))
		raise VolatileBlobFile('blob file {} keeps changing'.format(path))

	def __create_file(self, session: DbSession, backup: schema.Backup, path: Path) -> schema.File:
		related_path = path.relative_to(Config.get().source_path)
		st = path.stat()

		blob, content = None, None
		if stat.S_ISDIR(st.st_mode):
			pass
		elif stat.S_ISREG(st.st_mode):
			blob, st = self.__get_or_create_blob(session, path, st)
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
				backup = session.create_backup(
					author=self.author,
					comment=self.comment,
					targets=[str(Path(t).as_posix()) for t in self.config.backup.targets],
				)
				self.logger.info('Creating backup {}'.format(backup))
				for p in self.scan_files():
					self.__create_file(session, backup, p)
				self.logger.info('Create backup done, backup id {}'.format(backup.id))
				self.__backup_id = backup.id
		except Exception as e:
			self.logger.info('Error occurs, applying rollback')
			self.__backup_id = None
			for rollback_func in self.__blobs_rollbackers:
				rollback_func()
			raise e
