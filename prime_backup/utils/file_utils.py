import errno
import os
import shutil
import stat
from pathlib import Path
from typing import Optional

import psutil

from prime_backup.utils import path_utils

HAS_COPY_FILE_RANGE = callable(getattr(os, 'copy_file_range', None))


def __is_cow_not_supported_error(e: int) -> bool:
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
				while n := os.copy_file_range(f_src.fileno(), f_dst.fileno(), 2 ** 30):
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
