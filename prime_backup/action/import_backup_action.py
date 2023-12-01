import contextlib
import json
import shutil
import stat
import tarfile
import time
import zipfile
from pathlib import Path
from typing import ContextManager, IO, Optional, NamedTuple, List, Dict

from prime_backup.action.create_backup_action_base import CreateBackupActionBase
from prime_backup.compressors import Compressor
from prime_backup.constants import BACKUP_META_FILE_NAME
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.exceptions import PrimeBackupError
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.backup_meta import BackupMeta
from prime_backup.types.tar_format import TarFormat
from prime_backup.types.units import ByteCount
from prime_backup.utils import hash_utils, blob_utils, misc_utils
from prime_backup.utils.bypass_io import ByPassWriter
from prime_backup.utils.hash_utils import SizeAndHash


class UnsupportedFormat(PrimeBackupError):
	pass


class _FileDescription(NamedTuple):
	blob: Optional[schema.Blob]
	hash: str
	size: int


class ImportBackupAction(CreateBackupActionBase):
	def __init__(self, file_path: Path):
		super().__init__()
		self.file_path = file_path
		self.__blob_cache: Dict[str, schema.Blob] = {}

	@contextlib.contextmanager
	def __open_tar(self, tar_format: TarFormat) -> ContextManager[tarfile.TarFile]:
		with open(self.file_path, 'rb') as f:
			compressor = Compressor.create(tar_format.value.compress_method)
			with compressor.compress_stream(f) as f_compressed:
				with tarfile.open(fileobj=f_compressed, mode=tar_format.value.mode_r) as tar:
					yield tar

	@contextlib.contextmanager
	def __open_zip(self) -> ContextManager[zipfile.ZipFile]:
		with zipfile.ZipFile(self.file_path, 'r') as f:
			yield f

	def __get_or_create_blob(self, session: DbSession, file_reader: IO[bytes], sah: SizeAndHash) -> schema.Blob:
		if (blob := self.__blob_cache.get(sah.hash)) is not None:
			return blob

		blob_path = blob_utils.get_blob_path(sah.hash)
		self._add_remove_file_rollbacker(blob_path)

		compress_method = self.config.backup.get_compress_method_from_size(sah.size)
		compressor = Compressor.create(compress_method)
		with open(blob_path, 'wb') as f:
			writer = ByPassWriter(f)
			with compressor.compress_stream(writer) as f_compressed:
				shutil.copyfileobj(file_reader, f_compressed)

		blob = self._create_blob(
			session,
			hash=sah.hash,
			compress=compress_method.name,
			raw_size=sah.size,
			stored_size=writer.get_write_len(),
		)
		self.__blob_cache[sah.hash] = blob
		return blob

	@classmethod
	def __format_path(cls, path: str) -> str:
		return str(Path(path).as_posix())

	def __import_tar_member(
			self, session: DbSession,
			tar: tarfile.TarFile, member: tarfile.TarInfo, backup: schema.Backup, now_ns: int,
			file_sah: Optional[SizeAndHash],
	):
		blob: Optional[schema.Blob] = None
		content: Optional[bytes] = None

		if member.isfile():
			misc_utils.assert_true(file_sah is not None, 'file_sah should not be None for files')
			blob = self.__get_or_create_blob(session, tar.extractfile(member), file_sah)
		elif member.isdir():
			pass
		elif member.issym():
			content = self.__format_path(member.linkpath).encode('utf8')
		else:
			raise NotImplementedError('type={} is not supported yet'.format(member.type))

		return session.create_file(
			backup_id=backup.id,
			path=self.__format_path(member.path),
			content=content,

			mode=member.mode,
			uid=member.uid,
			gid=member.gid,
			ctime_ns=now_ns,
			mtime_ns=member.mtime,
			atime_ns=member.mtime,

			blob=blob,
		)

	def __import_tar(self, session: DbSession, tar: tarfile.TarFile) -> schema.Backup:
		meta: Optional[BackupMeta] = None
		try:
			meta_reader = tar.extractfile(BACKUP_META_FILE_NAME)
		except KeyError:
			pass
		else:
			try:
				meta_dict = json.load(meta_reader)
				meta = BackupMeta.from_dict(meta_dict)
			except Exception as e:
				self.logger.error('Read backup meta from {!r} failed: {}'.format(BACKUP_META_FILE_NAME, e))
			else:
				self.logger.info('Read backup meta from {!r} ok'.format(BACKUP_META_FILE_NAME))
		if meta is None:
			meta = BackupMeta.get_default()
			meta.targets.clear()
			# TODO: check meta.targets for normal ones too
			for member in tar.getmembers():  # scan all items at root dir
				item: str = member.path
				if item not in ('', '.', '..', BACKUP_META_FILE_NAME) and (item.count('/') == 0 or (item.count('/') == 1 and item.endswith('/'))):
					meta.targets.append(item)
			self.logger.info('No valid backup meta, generating a default one, target: {}'.format(meta.targets))

		backup = session.create_backup(**meta.to_backup_kwargs())

		self.logger.info('Importing backup {} from tarfile'.format(backup))
		now_ns = time.time_ns()
		members: List[tarfile.TarInfo] = [member for member in tar.getmembers() if member.path != BACKUP_META_FILE_NAME]

		sah_dict: Dict[int, SizeAndHash] = {}
		for i, member in enumerate(members):
			if member.isfile():
				sah_dict[i] = hash_utils.calc_reader_size_and_hash(tar.extractfile(member))

		blobs = session.get_blobs([sah.hash for sah in sah_dict.values()])
		for h, blob in blobs.items():
			self.__blob_cache[h] = blob

		blob_utils.prepare_blob_directories()
		for i, member in enumerate(members):
			self.__import_tar_member(session, tar, member, backup, now_ns, sah_dict.get(i))

		return backup

	@classmethod
	def __is_zip_member_file(cls, member: zipfile.ZipInfo) -> bool:
		mode = (member.external_attr >> 16) & 0xFFFF
		return not member.is_dir() and stat.S_ISREG(mode)

	def __import_zip_member(
			self, session: DbSession,
			zipf: zipfile.ZipFile, member: zipfile.ZipInfo, backup: schema.Backup, now_ns: int,
			file_sah: Optional[SizeAndHash],
	):
		blob: Optional[schema.Blob] = None
		content: Optional[bytes] = None

		mode = (member.external_attr >> 16) & 0xFFFF

		if member.is_dir():
			pass
		elif stat.S_ISLNK(mode):
			target = zipf.read(member).decode('utf8')
			content = self.__format_path(target).encode('utf8')
		elif stat.S_ISREG(mode):  # treats as regular file
			misc_utils.assert_true(file_sah is not None, 'file_sah should not be None for files')
			with zipf.open(member, 'r') as f:
				blob = self.__get_or_create_blob(session, f, file_sah)
		else:
			raise NotImplementedError('mode={} is not supported yet'.format(mode))

		mtime = time.mktime(member.date_time + (0, 0, -1))
		return session.create_file(
			backup_id=backup.id,
			path=self.__format_path(member.filename),
			content=content,

			mode=mode,
			ctime_ns=now_ns,
			mtime_ns=mtime,
			atime_ns=mtime,

			blob=blob,
		)

	def __import_zip(self, session: DbSession, zipf: zipfile.ZipFile) -> schema.Backup:
		meta: Optional[BackupMeta] = None
		try:
			zip_info = zipf.getinfo(BACKUP_META_FILE_NAME)
		except KeyError:
			pass
		else:
			try:
				meta_content = zipf.read(zip_info)
				meta_dict = json.loads(meta_content)
				meta = BackupMeta.from_dict(meta_dict)
			except Exception as e:
				self.logger.error('Read backup meta from {!r} failed: {}'.format(BACKUP_META_FILE_NAME, e))
			else:
				self.logger.info('Read backup meta from {!r} ok'.format(BACKUP_META_FILE_NAME))
		if meta is None:
			meta = BackupMeta.get_default()
			meta.targets.clear()
			for item in zipf.namelist():  # scan all items at root dir
				if item not in ('', '.', '..', BACKUP_META_FILE_NAME) and (item.count('/') == 0 or (item.count('/') == 1 and item.endswith('/'))):
					meta.targets.append(item)
			self.logger.info('No valid backup meta, generated a default one, target: {}'.format(meta.targets))

		backup = session.create_backup(**meta.to_backup_kwargs())

		self.logger.info('Importing backup {} from zipfile'.format(backup))
		now_ns = time.time_ns()
		members: List[zipfile.ZipInfo] = [member for member in zipf.infolist() if member.filename != BACKUP_META_FILE_NAME]

		sah_dict: Dict[int, SizeAndHash] = {}
		for i, member in enumerate(members):
			if self.__is_zip_member_file(member):
				with zipf.open(member, 'r') as f:
					sah_dict[i] = hash_utils.calc_reader_size_and_hash(f)

		blobs = session.get_blobs([sah.hash for sah in sah_dict.values()])
		for h, blob in blobs.items():
			self.__blob_cache[h] = blob

		blob_utils.prepare_blob_directories()
		for i, member in enumerate(members):
			self.__import_zip_member(session, zipf, member, backup, now_ns, sah_dict.get(i))

		return backup

	def run(self) -> BackupInfo:
		for tar_format in TarFormat:
			if self.file_path.name.endswith(tar_format.value.extension):
				break
		else:
			if self.file_path.name.endswith('.zip'):
				tar_format = None
			else:
				raise UnsupportedFormat(self.file_path.name)

		super().run()
		self.__blob_cache.clear()

		try:
			with DbAccess.open_session() as session:
				# TODO: abstract tar & zip, less duplicated codes
				if tar_format is not None:
					with self.__open_tar(tar_format) as tar:
						backup = self.__import_tar(session, tar)
				else:  # zip
					with self.__open_zip() as zipf:
						backup = self.__import_zip(session, zipf)

				info = BackupInfo.of(backup)

			s = self._summarize_new_blobs()
			self.logger.info('Import backup #{} done, +{} blobs (size {} / {})'.format(info.id, s.count, ByteCount(s.stored_size), ByteCount(s.raw_size)))
			return info

		except Exception:
			self._apply_blob_rollback()
			raise
