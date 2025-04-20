import collections
import dataclasses
import errno
import logging
import os
import random
import stat
import threading
import time
from pathlib import Path
from typing import Tuple, List, Optional, Union, Dict

import fuse

from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.action.get_db_overview_action import GetDbOverviewAction
from prime_backup.action.get_file_action import GetBackupFileAction, ListBackupDirectoryFileAction, NotDirectoryError, GetBackupFilesAction
from prime_backup.action.list_backup_action import ListBackupIdAction
from prime_backup.cli.fuse.cache import ttl_lru_cache, TTLLRUCounter, TTLLRUCache
from prime_backup.cli.fuse.common import PrimeBackupFuseStat, PrimeBackupFuseDirentry, PrimeBackupFuseStatVfs
from prime_backup.cli.fuse.config import FuseConfig
from prime_backup.cli.fuse.file import PrimeBackupFuseFile
from prime_backup.cli.fuse.utils import fuse_operation_wrapper, FuseErrnoReturnError
from prime_backup.constants import BACKUP_META_FILE_NAME
from prime_backup.exceptions import BackupFileNotFound, BackupNotFound
from prime_backup.logger import get as get_logger
from prime_backup.types.file_info import FileInfo
from prime_backup.utils.backup_id_parser import BackupIdParser, BackupIdAlternatives

fuse.fuse_python_api = (0, 2)


@dataclasses.dataclass(frozen=True)
class _NiceBackupFiles:
	by_path: Dict[str, FileInfo]
	by_parent: Dict[str, List[FileInfo]]


@dataclasses.dataclass(frozen=True)
class _SplitPathResult:
	backup_id: int
	path: str
	is_alt: bool


class _Helper:
	__FILE_BATCH_QUERY_THRESHOLD = 3
	__QUERY_BACKUP_DIR_FILES_THRESHOLD = 3

	def __init__(self):
		self.logger: logging.Logger = get_logger()
		self.__file_query_counter: TTLLRUCounter[int] = TTLLRUCounter(capacity=128, ttl=1)
		self.__query_backup_dir_files_counter: TTLLRUCounter[int] = TTLLRUCounter(capacity=128, ttl=1)

		if FuseConfig.get().no_cache:
			self.__parse_backup_id = self.__parse_backup_id_no_cache
			self.__query_backup_files = self.__parse_backup_id_no_cache
		else:
			self.__parse_backup_id = ttl_lru_cache(ttl=1, capacity=128)(self.__parse_backup_id_no_cache)
			self.__query_backup_files = ttl_lru_cache(ttl=1, capacity=4)(self.__query_backup_files_no_cache)
			threading.Thread(name='CacheCleaner', target=self.__cache_cleaner_thread, daemon=True).start()

	@staticmethod
	def __parse_backup_id_no_cache(s: str) -> Optional[int]:
		try:
			return BackupIdParser(allow_db_access=True).parse(s)
		except ValueError:
			return None

	@staticmethod
	def __query_backup_files_no_cache(backup_id: int) -> Optional[_NiceBackupFiles]:
		try:
			files = GetBackupFilesAction(backup_id).run()
		except ValueError:
			return None
		by_parent: Dict[str, List[FileInfo]] = collections.defaultdict(list)
		for path, file in files.items():
			if '/' in path:
				parent = path.rsplit('/', 1)[0]
			else:
				parent = ''
			by_parent[parent].append(file)
		return _NiceBackupFiles(files, dict(by_parent))

	def query_backup_file(self, backup_id: int, path: str) -> FileInfo:
		if (nbf := self.__query_backup_files(backup_id)) is None:
			raise FuseErrnoReturnError(errno.ENOENT)
		if (file := nbf.by_path.get(path)) is None:
			raise FuseErrnoReturnError(errno.ENOENT)
		return file

	def split_path(self, fuse_path: str, *, allow_alternative: bool) -> _SplitPathResult:
		"""
		:raise: FuseErrnoReturnError(errno.ENOENT) if fuse_path == '/'
		"""
		parts = Path(fuse_path).parts
		if len(parts) <= 1 or parts[0] != '/':
			# len(parts) == 1: querying root path '/'
			raise FuseErrnoReturnError(errno.ENOENT)

		if allow_alternative:
			backup_id = self.__parse_backup_id(parts[1])
			if backup_id is None:
				raise FuseErrnoReturnError(errno.ENOENT)
		else:
			try:
				backup_id = int(parts[1])
			except ValueError:
				raise FuseErrnoReturnError(errno.ENOENT)

		path = '/'.join(parts[2:])
		is_alt = str(backup_id) != parts[1]
		if path != '' and is_alt:
			# disallow accessing "/latest/foobar"
			raise FuseErrnoReturnError(errno.ENOENT)
		return _SplitPathResult(backup_id, path, is_alt=is_alt)

	def get_backup_file(self, fuse_path_or_spr: Union[str, _SplitPathResult]) -> Tuple[int, str, FileInfo]:
		"""
		NOTES: only support numeric backup id
		:param fuse_path_or_spr: needs to be inside a backup directory
		"/"         ->  ENOENT
		"/123"      ->  ENOENT
		"/123/foo"  ->  try get from DB
		"""
		if isinstance(fuse_path_or_spr, _SplitPathResult):
			spr = fuse_path_or_spr
		elif isinstance(fuse_path_or_spr, str):
			spr = self.split_path(fuse_path_or_spr, allow_alternative=False)
		else:
			raise TypeError()
		if spr.path == '':
			raise FuseErrnoReturnError(errno.ENOENT)

		if not FuseConfig.get().no_cache and self.__file_query_counter.inc(spr.backup_id) >= self.__FILE_BATCH_QUERY_THRESHOLD:
			nbf = self.__query_backup_files(spr.backup_id)
			if nbf is None:
				raise FuseErrnoReturnError(errno.ENOENT)

			file = nbf.by_path.get(spr.path)
			if file is None:
				raise FuseErrnoReturnError(errno.ENOENT)
		else:
			try:
				file = GetBackupFileAction(spr.backup_id, spr.path).run()
			except (BackupNotFound, BackupFileNotFound):
				raise FuseErrnoReturnError(errno.ENOENT)

		return spr.backup_id, spr.path, file

	def query_backup_dir_files(self, backup_id: int, path: str):
		if not FuseConfig.get().no_cache and self.__query_backup_dir_files_counter.inc(backup_id) >= self.__QUERY_BACKUP_DIR_FILES_THRESHOLD:
			nbf = self.__query_backup_files(backup_id)
			if nbf is None:
				raise FuseErrnoReturnError(errno.ENOENT)
			return nbf.by_parent.get(path, [])
		else:
			try:
				return ListBackupDirectoryFileAction(backup_id, path).run()
			except BackupNotFound:
				raise FuseErrnoReturnError(errno.ENOENT)
			except NotDirectoryError:
				raise FuseErrnoReturnError(errno.ENOTDIR)

	def __cache_cleaner_thread(self):
		caches: List[TTLLRUCache] = [
			self.__parse_backup_id.cache,
			self.__query_backup_files.cache,
			self.__file_query_counter.cache,
			self.__query_backup_dir_files_counter.cache,
		]
		while True:
			time.sleep(60 + random.random())
			for cache in caches:
				cache.prune_all()


class PrimeBackupFuseFs(fuse.Fuse):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.logger: logging.Logger = get_logger()
		self.__helper = helper = _Helper()

		class PrimeBackupFuseFileWrapper(PrimeBackupFuseFile):
			@fuse_operation_wrapper(func_name='create_file')
			def __init__(self, fuse_path: str, flags: int, *_modes: int):
				if (flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)) != os.O_RDONLY:
					raise FuseErrnoReturnError(errno.EACCES)

				spr = helper.split_path(fuse_path, allow_alternative=False)
				if spr.path == BACKUP_META_FILE_NAME and not FuseConfig.get().no_meta:
					backup = GetBackupAction(spr.backup_id).run()
					super().__init__(buf=backup.create_meta_buf())
				else:
					backup_id, path, file = helper.get_backup_file(spr)
					if not file.is_file():
						raise FuseErrnoReturnError(errno.EINVAL)
					if file.blob is None:
						raise FuseErrnoReturnError(errno.EIO)
					super().__init__(blob=file.blob)

		# file_class has to be a real class
		# if it's a function, fuse will think that file stuffs are unimplemented
		self.file_class = PrimeBackupFuseFileWrapper

	@fuse_operation_wrapper()
	def getattr(self, fuse_path: str) -> fuse.Stat:
		if fuse_path == '/':
			return PrimeBackupFuseStat.create_plain_dir(time.time())

		spr = self.__helper.split_path(fuse_path, allow_alternative=True)
		if spr.path == '':
			backup = GetBackupAction(spr.backup_id).run()
			if spr.is_alt:
				return PrimeBackupFuseStat.create_symlink(backup.timestamp_us / 1e6)
			else:
				return PrimeBackupFuseStat.create_plain_dir(backup.timestamp_us / 1e6)
		elif spr.path == BACKUP_META_FILE_NAME:
			backup = GetBackupAction(spr.backup_id).run()
			size = len(backup.create_meta_buf())
			return PrimeBackupFuseStat.create_regular(size, 0o444, backup.timestamp_us / 1e6)

		if spr.is_alt:
			raise FuseErrnoReturnError(errno.ENOENT)

		_, _, file = self.__helper.get_backup_file(fuse_path)
		return PrimeBackupFuseStat.from_file_info(file)

	@fuse_operation_wrapper()
	def readdir(self, fuse_path: str, offset: int) -> List[fuse.Direntry]:
		if offset != 0:
			raise FuseErrnoReturnError(errno.EINVAL)
		if fuse_path == '/':
			backup_ids = ListBackupIdAction().run()
			result: List[fuse.Direntry] = []
			for backup_id in backup_ids:
				result.append(PrimeBackupFuseDirentry(str(backup_id), type=stat.S_IFDIR))
			for bia in BackupIdAlternatives:
				result.append(PrimeBackupFuseDirentry(bia.name, type=stat.S_IFLNK))
			return result

		spr = self.__helper.split_path(fuse_path, allow_alternative=False)
		files = self.__helper.query_backup_dir_files(spr.backup_id, spr.path)
		result: List[PrimeBackupFuseDirentry] = [
			PrimeBackupFuseDirentry(Path(file.path).name, type=stat.S_IFMT(file.mode))
			for file in sorted(files)
		]
		if spr.path == '':
			result.append(PrimeBackupFuseDirentry(BACKUP_META_FILE_NAME, type=stat.S_IFREG))
		return result

	@fuse_operation_wrapper()
	def readlink(self, fuse_path: str) -> str:
		spr = self.__helper.split_path(fuse_path, allow_alternative=True)
		if spr.is_alt:
			if spr.path != '':
				raise FuseErrnoReturnError(errno.ENOENT)
			return str(spr.backup_id)

		_, _, file = self.__helper.get_backup_file(spr)
		if not file.is_link():
			raise FuseErrnoReturnError(errno.EINVAL)

		return file.content.decode('utf8') or ''

	@fuse_operation_wrapper()
	def statfs(self) -> fuse.StatVfs:
		overview = GetDbOverviewAction().run()
		return PrimeBackupFuseStatVfs.from_db_overview(overview)
