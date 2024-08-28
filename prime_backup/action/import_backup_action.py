import contextlib
import functools
import json
import os
import shutil
import stat
import tarfile
import threading
import time
import zipfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import ContextManager, IO, Optional, List, Dict, Tuple

from prime_backup.action.create_backup_action_base import CreateBackupActionBase
from prime_backup.compressors import Compressor, CompressMethod
from prime_backup.config.config import Config
from prime_backup.constants import BACKUP_META_FILE_NAME
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.exceptions import PrimeBackupError
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.backup_meta import BackupMeta
from prime_backup.types.operator import Operator, PrimeBackupOperatorNames
from prime_backup.types.standalone_backup_format import StandaloneBackupFormat
from prime_backup.types.tar_format import TarFormat
from prime_backup.types.units import ByteCount
from prime_backup.utils import hash_utils, blob_utils, misc_utils
from prime_backup.utils.hash_utils import SizeAndHash


class UnsupportedFormat(PrimeBackupError):
	pass


class BackupMetadataNotFound(PrimeBackupError):
	pass


class BackupMetadataInvalid(PrimeBackupError):
	pass


class PackedBackupFileHandler(ABC):
	class Member(ABC):
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
		def mtime_ns(self) -> int:
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

	class FileHolder(ABC):
		@abstractmethod
		def get_member(self, path: str) -> Optional['PackedBackupFileHandler.Member']:
			...

		@abstractmethod
		def list_member(self) -> List['PackedBackupFileHandler.Member']:
			...

	@abstractmethod
	def open_file(self, path: Path) -> ContextManager[FileHolder]:
		...


class TarBackupHandler(PackedBackupFileHandler):
	class TarMember(PackedBackupFileHandler.Member):
		def __init__(self, tar: tarfile.TarFile, member: tarfile.TarInfo):
			self.tar = tar
			self.member = member

		@property
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
		def path(self) -> str:
			return self.member.path

		@property
		def uid(self) -> int:
			return self.member.uid

		@property
		def gid(self) -> int:
			return self.member.gid

		@property
		def mtime_ns(self) -> int:
			return self.member.mtime * 10 ** 9

		def is_file(self) -> bool:
			return self.member.isfile()

		def is_dir(self) -> bool:
			return self.member.isdir()

		def is_link(self) -> bool:
			return self.member.issym()

		def read_link(self) -> str:
			return self.member.linkpath

		@contextlib.contextmanager
		def open(self) -> ContextManager[IO[bytes]]:
			yield self.tar.extractfile(self.member)

	class TarFileHolder(PackedBackupFileHandler.FileHolder):
		def __init__(self, tar: tarfile.TarFile):
			self.tar = tar

		def get_member(self, path: str) -> Optional['TarBackupHandler.TarMember']:
			try:
				member = self.tar.getmember(path)
			except KeyError:
				return None
			else:
				return TarBackupHandler.TarMember(self.tar, member)

		def list_member(self) -> List['TarBackupHandler.TarMember']:
			return [TarBackupHandler.TarMember(self.tar, member) for member in self.tar.getmembers()]

	def __init__(self, tar_format: TarFormat):
		self.tar_format = tar_format

	@contextlib.contextmanager
	def open_file(self, path: Path) -> ContextManager[TarFileHolder]:
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


class ZipBackupHandler(PackedBackupFileHandler):
	class ZipMember(PackedBackupFileHandler.Member):
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
		def mode(self) -> int:
			return self.__mode

		@property
		def path(self) -> str:
			return self.member.filename

		@property
		def uid(self) -> Optional[int]:
			return None

		@property
		def gid(self) -> Optional[int]:
			return None

		@property
		def mtime_ns(self) -> int:
			return int(time.mktime(self.member.date_time + (0, 0, -1)) * 1e9)

		def is_file(self) -> bool:
			return not self.is_dir() and stat.S_ISREG(self.mode)

		def is_dir(self) -> bool:
			return self.member.is_dir()

		def is_link(self) -> bool:
			return not self.is_dir() and stat.S_ISLNK(self.mode)

		def read_link(self) -> str:
			max_link_size = 10240
			with self.open() as f:
				buf = f.read(max_link_size)
				if len(buf) == max_link_size:
					raise ValueError('symlink too large, read {} bytes, peek: {}'.format(len(buf), buf[:20]))
				return buf.decode('utf8')

		@contextlib.contextmanager
		def open(self) -> ContextManager[IO[bytes]]:
			with self.zipf.open(self.member, 'r') as f:
				yield f

	class ZipFileHolder(PackedBackupFileHandler.FileHolder):
		def __init__(self, zipf: zipfile.ZipFile):
			self.zipf = zipf

		def get_member(self, path: str) -> Optional['ZipBackupHandler.ZipMember']:
			try:
				member = self.zipf.getinfo(path)
			except KeyError:
				return None
			else:
				return ZipBackupHandler.ZipMember(self.zipf, member)

		def list_member(self) -> List['ZipBackupHandler.ZipMember']:
			return [ZipBackupHandler.ZipMember(self.zipf, member) for member in self.zipf.infolist()]

	@contextlib.contextmanager
	def open_file(self, path: Path) -> ContextManager[ZipFileHolder]:
		with zipfile.ZipFile(path, 'r') as f:
			yield self.ZipFileHolder(f)


class ImportBackupAction(CreateBackupActionBase):
	def __init__(
			self, file_path: Path, backup_format: Optional[StandaloneBackupFormat] = None, *,
			ensure_meta: bool = True, meta_override: Optional[dict] = None,
	):
		super().__init__()

		if backup_format is None:
			backup_format = StandaloneBackupFormat.from_file_name(file_path)
			if backup_format is None:
				raise ValueError('cannot infer backup format from {}'.format(file_path))

		self.file_path = file_path
		self.backup_format = backup_format
		self.ensure_meta = ensure_meta
		self.meta_override = meta_override

		self.__blob_cache: Dict[str, schema.Blob] = {}

	def __create_blob_file(self, file_reader: IO[bytes], sah: SizeAndHash) -> Tuple[int, CompressMethod]:
		blob_path = blob_utils.get_blob_path(sah.hash)
		self._add_remove_file_rollbacker(blob_path)

		compress_method: CompressMethod = self.config.backup.get_compress_method_from_size(sah.size)
		compressor = Compressor.create(compress_method)
		with compressor.open_compressed_bypassed(blob_path) as (writer, f):
			shutil.copyfileobj(file_reader, f)

		return writer.get_write_len(), compress_method

	def __create_blob(self, session: DbSession, file_reader: IO[bytes], sah: SizeAndHash) -> schema.Blob:
		stored_size, compress_method = self.__create_blob_file(file_reader, sah)
		blob = self._create_blob(
			session,
			hash=sah.hash,
			compress=compress_method.name,
			raw_size=sah.size,
			stored_size=stored_size,
		)
		self.__blob_cache[sah.hash] = blob
		return blob

	@classmethod
	def __format_path(cls, path: str) -> str:
		return Path(path).as_posix()

	def __import_member(
			self, session: DbSession,
			member: PackedBackupFileHandler.Member, now_ns: int,
			file_sah: Optional[SizeAndHash],
	):
		blob: Optional[schema.Blob] = None
		content: Optional[bytes] = None

		if member.is_file():
			misc_utils.assert_true(file_sah is not None, 'file_sah should not be None for files')
			if (blob := self.__blob_cache.get(file_sah.hash)) is None:
				with member.open() as f:
					blob = self.__create_blob(session, f, file_sah)
		elif member.is_dir():
			pass
		elif member.is_link():
			content = self.__format_path(member.read_link()).encode('utf8')
		else:
			raise NotImplementedError('member path={!r} mode={} is not supported yet'.format(member.path, member.mode))

		mtime_ns = member.mtime_ns
		return session.create_file(
			path=self.__format_path(member.path),
			content=content,

			mode=member.mode,
			uid=member.uid,
			gid=member.gid,
			ctime_ns=now_ns,
			mtime_ns=mtime_ns,
			atime_ns=mtime_ns,

			add_to_session=False,
			blob=blob,
		)

	def __import_packed_backup_file(self, session: DbSession, file_holder: PackedBackupFileHandler.FileHolder) -> schema.Backup:
		meta: Optional[BackupMeta] = None

		if self.meta_override is not None:
			try:
				meta = BackupMeta.from_dict(self.meta_override)
			except Exception as e:
				self.logger.error('Read backup meta from meta_override {!r} failed: {}'.format(self.meta_override, e))
				raise BackupMetadataInvalid(e)
		elif (meta_obj := file_holder.get_member(BACKUP_META_FILE_NAME)) is not None:
			with meta_obj.open() as meta_reader:
				try:
					meta_dict = json.load(meta_reader)
					meta = BackupMeta.from_dict(meta_dict)
				except Exception as e:
					self.logger.error('Read backup meta from {!r} failed: {}'.format(BACKUP_META_FILE_NAME, e))
					raise BackupMetadataInvalid(e)
				else:
					self.logger.info('Read backup meta from {!r} ok'.format(BACKUP_META_FILE_NAME))
		else:
			self.logger.info('The importing backup does not contain the backup meta file {!r}'.format(BACKUP_META_FILE_NAME))
			if self.ensure_meta:
				raise BackupMetadataNotFound('{} does not exist'.format(BACKUP_META_FILE_NAME))

		members: List[PackedBackupFileHandler.Member] = list(filter(
			lambda m: m.path != BACKUP_META_FILE_NAME,
			file_holder.list_member(),
		))
		root_files = []
		for member in members:
			item = member.path
			if item not in ('', '.', '..') and (item.count('/') == 0 or (item.count('/') == 1 and item.endswith('/'))):
				root_files.append(item.rstrip('/'))

		if meta is None:
			meta = BackupMeta.get_default()
			meta.targets = root_files
			self.logger.info('No valid backup meta, generating a default one, target: {}'.format(meta.targets))
		else:
			extra_files = list(sorted(set(root_files).difference(set(meta.targets))))
			if len(extra_files) > 0:
				self.logger.warning('Found extra files inside {!r}: {}. They are not included in the targets {}'.format(
					self.file_path.name, extra_files, meta.targets,
				))
		if meta.creator == str(Operator.unknown()):
			meta.creator = str(Operator.pb(PrimeBackupOperatorNames.import_))

		backup = session.create_backup(**meta.to_backup_kwargs())

		self.logger.info('Importing backup {} from {!r}'.format(backup, self.file_path.name))
		now_ns = time.time_ns()

		sah_dict: Dict[int, SizeAndHash] = {}
		for i, member in enumerate(members):
			if member.is_file():
				with member.open() as f:
					sah_dict[i] = hash_utils.calc_reader_size_and_hash(f)

		blobs = session.get_blobs([sah.hash for sah in sah_dict.values()])
		for h, blob in blobs.items():
			self.__blob_cache[h] = blob

		files = []
		blob_utils.prepare_blob_directories()
		for i, member in enumerate(members):
			try:
				file = self.__import_member(session, member, now_ns, sah_dict.get(i))
			except Exception as e:
				self.logger.error('Import member {!r} (mode {}) failed: {}'.format(member.path, member.mode, e))
				raise

			files.append(file)

		self._finalize_backup_and_files(session, backup, files)
		return backup

	def run(self) -> BackupInfo:
		if isinstance(self.backup_format.value, TarFormat):
			tar_format = self.backup_format.value
		else:
			tar_format = None

		super().run()
		self.__blob_cache.clear()

		try:
			with DbAccess.open_session() as session:
				handler: PackedBackupFileHandler
				if tar_format is not None:
					handler = TarBackupHandler(tar_format)
				else:  # zip
					handler = ZipBackupHandler()

				with handler.open_file(self.file_path) as file_holder:
					backup = self.__import_packed_backup_file(session, file_holder)
				info = BackupInfo.of(backup)

			s = self.get_new_blobs_summary()
			self.logger.info('Import backup #{} done, +{} blobs (size {} / {})'.format(
				info.id, s.count, ByteCount(s.stored_size).auto_str(), ByteCount(s.raw_size).auto_str(),
			))
			return info

		except Exception:
			self._apply_blob_rollback()
			raise
