import os
import shutil
from pathlib import Path
from typing import Optional

import psutil

from prime_backup.utils import path_utils

HAS_COPY_FILE_RANGE = callable(getattr(os, 'copy_file_range', None))


def copy_file_fast(src_path: Path, dst_path: Path):
	if HAS_COPY_FILE_RANGE:
		with open(src_path, 'rb') as f_src, open(dst_path, 'wb+') as f_dst:
			while os.copy_file_range(f_src.fileno(), f_dst.fileno(), 2 ** 30):
				pass
	else:
		shutil.copyfile(src_path, dst_path, follow_symlinks=False)


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
