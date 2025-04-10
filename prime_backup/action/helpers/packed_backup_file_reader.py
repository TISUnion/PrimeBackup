import contextlib
import functools
import os
import stat
import tarfile
import threading
import time
import zipfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import ContextManager, IO, Optional, List, Generator

from typing_extensions import override

from prime_backup.compressors import Compressor, CompressMethod
from prime_backup.config.config import Config
from prime_backup.types.tar_format import TarFormat


class PackedBackupFileMember(ABC):
	@property
	@abstractmethod
	def mode(self) -> int:
		...

	@property
	@abstractmethod
	def path(self) -> str:
		...

	@property
	@abstractmethod
	def uid(self) -> Optional[int]:
		...

	@property
	@abstractmethod
	def gid(self) -> Optional[int]:
		...

	@property
	@abstractmethod
	def mtime_us(self) -> int:
		...

	@abstractmethod
	def is_file(self) -> bool:
		...

	@abstractmethod
	def is_dir(self) -> bool:
		...

	@abstractmethod
	def is_link(self) -> bool:
		...

	@abstractmethod
	def open(self) -> ContextManager[IO[bytes]]:
		...

	@abstractmethod
	def read_link(self) -> str:
		...


class PackedBackupFileHolder(ABC):
	@abstractmethod
	def get_member(self, path: str) -> Optional[PackedBackupFileMember]:
		...

	@abstractmethod
	def list_member(self) -> List[PackedBackupFileMember]:
		...


class PackedBackupFileReader(ABC):
	@abstractmethod
	def open_file(self, path: Path) -> ContextManager[PackedBackupFileHolder]:
		...


class TarBackupReader(PackedBackupFileReader):
	class TarMember(PackedBackupFileMember):
		def __init__(self, tar: tarfile.TarFile, member: tarfile.TarInfo):
			self.tar = tar
			self.member = member

		@property
		@override
		def mode(self) -> int:
			mode = self.member.mode & 0xFFFF
			if self.member.isfile():
				mode |= stat.S_IFREG
			elif self.member.isdir():
				mode |= stat.S_IFDIR
			elif self.member.issym():
				mode |= stat.S_IFLNK
			else:
				raise NotImplementedError('not implemented for type {}'.format(self.member.type))
			return mode

		@property
		@override
		def path(self) -> str:
			return self.member.path

		@property
		@override
		def uid(self) -> int:
			return self.member.uid

		@property
		@override
		def gid(self) -> int:
			return self.member.gid

		@property
		@override
		def mtime_us(self) -> int:
			return self.member.mtime * (10 ** 6)

		@override
		def is_file(self) -> bool:
			return self.member.isfile()

		@override
		def is_dir(self) -> bool:
			return self.member.isdir()

		@override
		def is_link(self) -> bool:
			return self.member.issym()

		@override
		def read_link(self) -> str:
			return self.member.linkpath

		@contextlib.contextmanager
		@override
		def open(self) -> Generator[IO[bytes], None, None]:
			yield self.tar.extractfile(self.member)

	class TarFileHolder(PackedBackupFileHolder):
		def __init__(self, tar: tarfile.TarFile):
			self.tar = tar

		@override
		def get_member(self, path: str) -> Optional['TarBackupReader.TarMember']:
			try:
				member = self.tar.getmember(path)
			except KeyError:
				return None
			else:
				return TarBackupReader.TarMember(self.tar, member)

		@override
		def list_member(self) -> List['TarBackupReader.TarMember']:
			return [TarBackupReader.TarMember(self.tar, member) for member in self.tar.getmembers()]

	def __init__(self, tar_format: TarFormat):
		self.tar_format = tar_format

	@contextlib.contextmanager
	@override
	def open_file(self, path: Path) -> Generator[TarFileHolder, None, None]:
		compress_method = self.tar_format.value.compress_method
		if compress_method == CompressMethod.plain:
			with tarfile.open(path, mode=self.tar_format.value.mode_r) as tar:
				yield self.TarFileHolder(tar)
		else:
			# zstd stream does not support seek operation, sowe need to extract the tar into a temp path first,
			# then operate on it. requires extra spaces tho

			temp_file = Config.get().temp_path / 'import_{}_{}.tmp'.format(os.getpid(), threading.current_thread().ident)
			temp_file.parent.mkdir(parents=True, exist_ok=True)
			with contextlib.ExitStack() as exit_stack:
				exit_stack.callback(functools.partial(temp_file.unlink, missing_ok=True))
				Compressor.create(compress_method).copy_decompressed(path, temp_file)

				with tarfile.open(temp_file, mode=self.tar_format.value.mode_r) as tar:
					yield self.TarFileHolder(tar)


class ZipBackupReader(PackedBackupFileReader):
	class ZipMember(PackedBackupFileMember):
		def __init__(self, zipf: zipfile.ZipFile, member: zipfile.ZipInfo):
			self.zipf = zipf
			self.member = member

			mode = (self.member.external_attr >> 16) & 0xFFFF
			if mode == 0:
				if self.path.endswith('/'):
					mode = stat.S_IFDIR | 0o755
				else:
					mode = stat.S_IFREG | 0o644
			self.__mode = mode

		@property
		@override
		def mode(self) -> int:
			return self.__mode

		@property
		@override
		def path(self) -> str:
			return self.member.filename

		@property
		@override
		def uid(self) -> Optional[int]:
			return None

		@property
		@override
		def gid(self) -> Optional[int]:
			return None

		@property
		@override
		def mtime_us(self) -> int:
			return int(time.mktime(self.member.date_time + (0, 0, -1)) * 1e6)

		@override
		def is_file(self) -> bool:
			return not self.is_dir() and stat.S_ISREG(self.mode)

		@override
		def is_dir(self) -> bool:
			return self.member.is_dir()

		@override
		def is_link(self) -> bool:
			return not self.is_dir() and stat.S_ISLNK(self.mode)

		@override
		def read_link(self) -> str:
			max_link_size = 10240
			with self.open() as f:
				buf = f.read(max_link_size)
				if len(buf) == max_link_size:
					raise ValueError('symlink too large, read {} bytes, peek: {}'.format(len(buf), buf[:20]))
				return buf.decode('utf8')

		@contextlib.contextmanager
		@override
		def open(self) -> Generator[IO[bytes], None, None]:
			with self.zipf.open(self.member, 'r') as f:
				yield f

	class ZipFileHolder(PackedBackupFileHolder):
		def __init__(self, zipf: zipfile.ZipFile):
			self.zipf = zipf

		@override
		def get_member(self, path: str) -> Optional['ZipBackupReader.ZipMember']:
			try:
				member = self.zipf.getinfo(path)
			except KeyError:
				return None
			else:
				return ZipBackupReader.ZipMember(self.zipf, member)

		@override
		def list_member(self) -> List['ZipBackupReader.ZipMember']:
			return [ZipBackupReader.ZipMember(self.zipf, member) for member in self.zipf.infolist()]

	@contextlib.contextmanager
	@override
	def open_file(self, path: Path) -> Generator[ZipFileHolder, None, None]:
		with zipfile.ZipFile(path, 'r') as f:
			yield self.ZipFileHolder(f)
