import os
import stat

import fuse
import math
from typing_extensions import Self

from prime_backup.action.get_db_overview_action import DbOverviewResult
from prime_backup.types.file_info import FileInfo
from prime_backup.utils import misc_utils


class PrimeBackupFuseStat(fuse.Stat):
	def __repr__(self):
		return misc_utils.represent(self, attrs={
			'st_mode': self.st_mode,
			'st_ino': self.st_ino,
			'st_dev': self.st_dev,
			'st_nlink': self.st_nlink,
			'st_uid': self.st_uid,
			'st_gid': self.st_gid,
			'st_size': self.st_size,
			'st_atime': self.st_atime,
			'st_mtime': self.st_mtime,
			'st_ctime': self.st_ctime,
		})

	@classmethod
	def from_file_info(cls, file: FileInfo) -> Self:
		st = cls()
		st.st_mode = file.mode
		st.st_nlink = 1  # XXX: nlink?
		st.st_size = file.blob.raw_size if file.blob else 0
		st.st_uid = file.uid or 0
		st.st_gid = file.gid or 0

		mtime = (file.mtime_us or 0) / 1e6
		st.st_atime = mtime
		st.st_mtime = mtime
		st.st_ctime = mtime

		return st

	@classmethod
	def create_regular(cls, size: int, mode: int, mtime: float) -> Self:
		st = cls()
		st.st_size = size
		st.st_mode = mode | stat.S_IFREG
		st.st_nlink = 1
		st.st_atime = st.st_ctime = st.st_mtime = mtime
		return st

	@classmethod
	def create_plain_dir(cls, mtime: float) -> Self:
		st = cls()
		st.st_mode = 0o755 | stat.S_IFDIR
		st.st_nlink = 1
		st.st_atime = st.st_ctime = st.st_mtime = mtime
		return st

	@classmethod
	def create_symlink(cls, mtime: float) -> Self:
		st = cls()
		st.st_mode = 0o777 | stat.S_IFLNK
		st.st_nlink = 1
		st.st_atime = st.st_ctime = st.st_mtime = mtime
		return st


class PrimeBackupFuseDirentry(fuse.Direntry):
	def __repr__(self):
		return misc_utils.represent(self, attrs={
			'name': self.name,
			'offset': self.offset,
			'type': self.type,
			'ino': self.ino,
		})


class PrimeBackupFuseStatVfs(fuse.StatVfs):
	def __repr__(self):
		return misc_utils.represent(self, attrs={
			'f_bsize': self.f_bsize,
			'f_frsize': self.f_frsize,
			'f_blocks': self.f_blocks,
			'f_bfree': self.f_bfree,
			'f_bavail': self.f_bavail,
			'f_files': self.f_files,
			'f_ffree': self.f_ffree,
			'f_favail': self.f_favail,
			'f_flag': self.f_flag,
			'f_namemax': self.f_namemax,
		})

	@classmethod
	def from_db_overview(cls, overview: DbOverviewResult) -> Self:
		block_size = 4096  # 4KB block size

		# https://man7.org/linux/man-pages/man3/statvfs.3.html
		st = cls()
		st.f_bsize = block_size  # block size
		st.f_frsize = block_size  # fragment size (same as block size)
		st.f_blocks = math.ceil(overview.file_raw_size_sum / block_size)  # total block count
		st.f_bfree = 0  # free block count
		st.f_bavail = 0  # available block count
		st.f_files = overview.file_total_count  # inode count (equal to number of files)
		st.f_ffree = 0  # free inode count
		st.f_favail = 0  # free inode count
		st.f_flag = os.ST_NOATIME | os.ST_NODEV | os.ST_NODIRATIME | os.ST_RDONLY  # flags
		st.f_namemax = 255  # maximum filename length
		return st
