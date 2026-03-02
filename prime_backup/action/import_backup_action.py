import json
from pathlib import Path
from typing import IO, Optional, List, Dict, Tuple

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.action.helpers.backup_finalizer import BackupFinalizer
from prime_backup.action.helpers.blob_pre_calc_result import BlobPrecalculateResult
from prime_backup.action.helpers.blob_recorder import BlobRecorder
from prime_backup.action.helpers.chunk_grouper import ChunkGrouper
from prime_backup.action.helpers.packed_backup_file_reader import PackedBackupFileReader, TarBackupReader, ZipBackupReader, PackedBackupFileMember, PackedBackupFileHolder
from prime_backup.compressors import Compressor, CompressMethod
from prime_backup.constants.constants import BACKUP_META_FILE_NAME
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.db.values import FileRole, BlobStorageMethod
from prime_backup.exceptions import PrimeBackupError
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.backup_meta import BackupMeta
from prime_backup.types.operator import Operator, PrimeBackupOperatorNames
from prime_backup.types.standalone_backup_format import StandaloneBackupFormat
from prime_backup.types.tar_format import TarFormat
from prime_backup.types.units import ByteCount
from prime_backup.utils import blob_utils, misc_utils, chunk_utils, collection_utils, file_utils
from prime_backup.utils.hash_utils import SizeAndHash


class UnsupportedFormat(PrimeBackupError):
	pass


class BackupMetadataNotFound(PrimeBackupError):
	pass


class BackupMetadataInvalid(PrimeBackupError):
	pass


class ImportBackupAction(Action[BackupInfo]):
	def __init__(
			self, file_path: Path, backup_format: Optional[StandaloneBackupFormat] = None, *,
			ensure_meta: bool = True, meta_override: Optional[dict] = None,
	):
		super().__init__()

		if backup_format is None:
			backup_format = StandaloneBackupFormat.from_file_name(file_path)
			if backup_format is None:
				raise UnsupportedFormat('cannot infer backup format from {!r}'.format(file_path))

		self.file_path = file_path
		self.backup_format = backup_format
		self.ensure_meta = ensure_meta
		self.meta_override = meta_override

		self.__blob_cache: Dict[str, schema.Blob] = {}
		self.__chunk_cache: Dict[str, schema.Chunk] = {}
		self.__blob_recorder = BlobRecorder()

	def __create_blob_file(self, file_reader: IO[bytes], sah: SizeAndHash) -> Tuple[int, CompressMethod]:
		blob_path = blob_utils.get_blob_path(sah.hash)
		self.__blob_recorder.add_remove_file_rollbacker(blob_path)

		compress_method: CompressMethod = self.config.backup.get_compress_method_from_size(sah.size)
		compressor = Compressor.create(compress_method)
		with compressor.open_compressed_bypassed(blob_path) as (writer, f):
			file_utils.copy_file_obj_fast(file_reader, f, estimate_read_size=sah.size)

		return writer.get_write_len(), compress_method

	def __create_blob_direct(self, session: DbSession, file_reader: IO[bytes], sah: SizeAndHash) -> schema.Blob:
		stored_size, compress_method = self.__create_blob_file(file_reader, sah)
		return self.__blob_recorder.create_blob(
			session,
			hash=sah.hash,
			compress=compress_method.name,
			raw_size=sah.size,
			stored_size=stored_size,
			storage_method=BlobStorageMethod.direct.value,
		)

	def __create_chunk_file(self, data: memoryview, sah: SizeAndHash) -> Tuple[int, CompressMethod]:
		if len(data) != sah.size:
			raise ValueError()
		chunk_path = chunk_utils.get_chunk_path(sah.hash)
		self.__blob_recorder.add_remove_file_rollbacker(chunk_path)

		compress_method: CompressMethod = self.config.backup.get_compress_method_from_size(sah.size)
		compressor = Compressor.create(compress_method)
		with compressor.open_compressed_bypassed(chunk_path) as (writer, f):
			f.write(data)

		return writer.get_write_len(), compress_method

	def __create_chunk(self, session: DbSession, data: memoryview, sah: SizeAndHash) -> schema.Chunk:
		stored_size, compress_method = self.__create_chunk_file(data, sah)
		chunk = session.create_chunk(
			hash=sah.hash,
			compress=compress_method.name,
			raw_size=sah.size,
			stored_size=stored_size,
		)
		self.__chunk_cache[sah.hash] = chunk
		return chunk

	def __create_blob_chunked(self, session: DbSession, file_reader: IO[bytes], pre_cal_result: BlobPrecalculateResult) -> schema.Blob:
		new_db_chunks: List[schema.Chunk] = []
		offset_to_db_chunk: Dict[int, schema.Chunk] = {}
		offset = 0
		for chunk in chunk_utils.StreamChunker(file_reader, need_entire_file_hash=False).cut():
			if (db_chunk := self.__chunk_cache.get(chunk.hash)) is None:
				db_chunk = self.__create_chunk(session, chunk.data, SizeAndHash(chunk.length, chunk.hash))
				new_db_chunks.append(db_chunk)
			offset_to_db_chunk[offset] = db_chunk
			offset += chunk.length

		for new_db_chunk in new_db_chunks:
			self.__blob_recorder.record_chunk(new_db_chunk)
			session.add(new_db_chunk)
		blob = self.__blob_recorder.create_blob(
			session,
			hash=pre_cal_result.hash,
			compress=CompressMethod.plain.name,
			raw_size=pre_cal_result.size,
			stored_size=sum({db_chunk.hash: db_chunk.stored_size for db_chunk in offset_to_db_chunk.values()}.values()),
			storage_method=BlobStorageMethod.chunked.value,
		)
		session.flush()  # creates blob.id, chunk.id
		ChunkGrouper(session, None).create_chunk_groups(blob, offset_to_db_chunk)

		return blob

	def __create_blob(self, session: DbSession, file_path: str, file_reader: IO[bytes], pre_cal_result: BlobPrecalculateResult) -> schema.Blob:
		if chunk_utils.should_chunk_blob(Path(file_path), pre_cal_result.size):
			blob = self.__create_blob_chunked(session, file_reader, pre_cal_result)
		else:
			blob = self.__create_blob_direct(session, file_reader, SizeAndHash(pre_cal_result.size, pre_cal_result.hash))

		self.__blob_cache[blob.hash] = blob
		return blob

	@classmethod
	def __format_path(cls, path: str) -> str:
		return Path(path).as_posix()

	def __import_member(
			self, session: DbSession,
			member: PackedBackupFileMember,
			pre_cal_result: Optional[BlobPrecalculateResult],
	):
		blob: Optional[schema.Blob] = None
		content: Optional[bytes] = None

		if member.is_file():
			misc_utils.assert_true(pre_cal_result is not None, 'file_sah should not be None for files')
			assert pre_cal_result is not None  # make mypy happy
			if (blob := self.__blob_cache.get(pre_cal_result.hash)) is None:
				with member.open() as f:
					blob = self.__create_blob(session, member.path, f, pre_cal_result)
		elif member.is_dir():
			pass
		elif member.is_link():
			content = self.__format_path(member.read_link()).encode('utf8')
		else:
			raise NotImplementedError('member path={!r} mode={} is not supported yet'.format(member.path, member.mode))

		return session.create_file(
			path=self.__format_path(member.path),
			content=content,
			role=FileRole.unknown.value,

			mode=member.mode,
			uid=member.uid,
			gid=member.gid,
			mtime=member.mtime_ns // (10 ** 9),
			mtime_ns_part=member.mtime_ns % (10 ** 9),

			blob=blob,
		)

	def __import_packed_backup_file(self, session: DbSession, file_holder: PackedBackupFileHolder) -> schema.Backup:
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

		members: List[PackedBackupFileMember] = list(filter(
			lambda m: m.path != BACKUP_META_FILE_NAME,
			file_holder.list_member(),
		))
		root_files = []
		for member in members:
			item = member.path
			if item not in ('', '.', '..') and (item.count('/') == 0 or (item.count('/') == 1 and item.endswith('/'))):
				root_files.append(item.rstrip('/'))

		if meta is None:
			meta = BackupMeta(
				targets=root_files,
			)
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

		pre_cal_dict: Dict[int, BlobPrecalculateResult] = {}
		for i, member in enumerate(members):
			if member.is_file():
				with member.open() as f:
					pre_cal_dict[i] = BlobPrecalculateResult.from_stream(f, Path(member.path), member.size)

		for h, blob in session.get_blobs_by_hashes_opt([res.hash for res in pre_cal_dict.values()]).items():
			if blob is not None:
				self.__blob_cache[h] = blob

		for h, chunk in session.get_chunks_by_hashes_opt(collection_utils.deduplicated_list(
			c.hash for res in pre_cal_dict.values() for c in res.chunks
		)).items():
			if chunk is not None:
				self.__chunk_cache[h] = chunk

		files: List[schema.File] = []
		blob_utils.prepare_blob_directories()
		chunk_utils.prepare_chunk_directories()
		for i, member in enumerate(members):
			try:
				file = self.__import_member(session, member, pre_cal_dict.get(i))
			except Exception as e:
				self.logger.error('Import member {!r} (mode {}) failed: {}'.format(member.path, member.mode, e))
				raise

			files.append(file)

		BackupFinalizer(session).finalize_files_and_backup(backup, files)
		return backup

	@override
	def run(self) -> BackupInfo:
		if isinstance(self.backup_format.value, TarFormat):
			tar_format = self.backup_format.value
		else:
			tar_format = None

		self.__blob_cache.clear()

		try:
			with DbAccess.open_session() as session:
				handler: PackedBackupFileReader
				if tar_format is not None:
					handler = TarBackupReader(tar_format)
				else:  # zip
					handler = ZipBackupReader()

				with handler.open_file(self.file_path) as file_holder:
					backup = self.__import_packed_backup_file(session, file_holder)
				info = BackupInfo.of(backup)

			bds = self.__blob_recorder.get_blob_storage_delta()
			self.logger.info('Import backup #{} done, added {} blobs and {} chunks (size {} / {})'.format(
				info.id, bds.blob_count, bds.chunk_count, ByteCount(bds.stored_size).auto_str(), ByteCount(bds.raw_size).auto_str(),
			))
			return info

		except Exception:
			self.__blob_recorder.apply_file_rollback()
			raise
