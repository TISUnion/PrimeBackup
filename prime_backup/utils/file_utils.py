import errno
import functools
import os
import queue
import shutil
import stat
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

import psutil

from prime_backup.utils import path_utils
from prime_backup.utils.io_types import SupportsReadBytes, SupportsWriteBytes

HAS_COPY_FILE_RANGE = callable(getattr(os, 'copy_file_range', None))


def __is_cow_not_supported_error(e: Optional[int]) -> bool:
	# https://github.com/coreutils/coreutils/blob/c343bee1b5de6087b70fe80db9e1f81bb1fc535c/src/copy.c#L292
	return e in (
		errno.ENOSYS, errno.ENOTTY, errno.EOPNOTSUPP, errno.ENOTSUP,
		errno.EINVAL, errno.EBADF,
		errno.EXDEV, errno.ETXTBSY,
		errno.EPERM, errno.EACCES,
	)


def copy_file_fast(src_path: Path, dst_path: Path):
	# https://man7.org/linux/man-pages/man2/copy_file_range.2.html
	if HAS_COPY_FILE_RANGE:
		total_read = 0
		try:
			with open(src_path, 'rb') as f_src, open(dst_path, 'wb+') as f_dst:
				while n := os.copy_file_range(f_src.fileno(), f_dst.fileno(), 2 ** 30):  # type: ignore[attr-defined]
					total_read += n
			return
		except OSError as e:
			# unsupported or read nothing -> retry with shutil.copyfile
			# reference: https://github.com/coreutils/coreutils/blob/c343bee1b5de6087b70fe80db9e1f81bb1fc535c/src/copy.c#L312
			if __is_cow_not_supported_error(e.errno) and total_read == 0:
				pass
			else:
				raise

	shutil.copyfile(src_path, dst_path, follow_symlinks=False)


class _ThreadedFastFileObjCopier:
	COPY_BUFSIZE = 1024 * 1024 if os.name == 'nt' else 4 * 1024  # == shutil.COPY_BUFSIZE
	MEMORY_CACHE_SIZE = 8 * 1048576

	def __init__(self, concurrency: int):
		self.concurrency = concurrency
		self.thread_pool = ThreadPoolExecutor(max_workers=concurrency)

	def copy(self, src: SupportsReadBytes, dst: SupportsWriteBytes):
		q: 'queue.Queue[Optional[bytes]]' = queue.Queue(maxsize=max(1, self.MEMORY_CACHE_SIZE // self.COPY_BUFSIZE))
		func_exited = False

		# reference: shutil.copyfileobj
		q_put = q.put
		q_get = q.get
		buf_size = self.COPY_BUFSIZE
		read_func = src.read
		write_func = dst.write

		def read_worker():
			try:
				while not func_exited and (read_buf := read_func(buf_size)):
					q_put(read_buf)
			finally:
				q_put(None)

		future = self.thread_pool.submit(read_worker)
		try:
			while (write_buf := q_get()) is not None:
				write_func(write_buf)
			future.result()
		finally:
			func_exited = True


@functools.lru_cache(None)
def __get_copier():
	from prime_backup.config.config import Config
	return _ThreadedFastFileObjCopier(Config.get().get_effective_concurrency())


def copy_file_obj_fast(src: SupportsReadBytes, dst: SupportsWriteBytes, *, estimate_read_size: int = 0):
	if estimate_read_size > 1048576 and False:  # TODO
		__get_copier().copy(src, dst)
	else:
		shutil.copyfileobj(src, dst)


def rm_rf(path: Path, *, missing_ok: bool = False):
	"""
	Does not follow symlink
	"""
	try:
		is_dir = stat.S_ISDIR(path.lstat().st_mode)
	except FileNotFoundError:
		if not missing_ok:
			raise
	else:
		if is_dir:
			shutil.rmtree(path)
		else:
			path.unlink(missing_ok=missing_ok)


def does_fs_support_cow(path: Path) -> bool:
	path = path.absolute()
	mount_point: Optional[str] = None
	fs_type = '?'
	for p in psutil.disk_partitions():
		if p.mountpoint and p.fstype and path_utils.is_relative_to(path, p.mountpoint):
			if mount_point is None or path_utils.is_relative_to(Path(p.mountpoint), mount_point):
				mount_point = p.mountpoint
				fs_type = p.fstype.lower()
	# zfs does not support COW copy yet: https://github.com/openzfs/zfs/issues/405
	return fs_type in ['xfs', 'btrfs', 'apfs']
