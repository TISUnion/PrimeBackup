import contextlib
import functools
import shutil
import sqlite3
import time
from pathlib import Path
from typing import Optional, Sequence, Dict, ContextManager, Iterator, Callable
from typing import TypeVar, List

from sqlalchemy import select, delete, desc, func, Select, JSON, text, or_
from sqlalchemy.orm import Session
from typing_extensions import overload, Union, TypedDict, Unpack, NotRequired

from prime_backup.db import schema, db_constants
from prime_backup.db.values import FileRole, BackupTagDict
from prime_backup.exceptions import BackupNotFound, BackupFileNotFound, BlobNotFound, PrimeBackupError, FilesetNotFound, FilesetFileNotFound
from prime_backup.types.backup_filter import BackupFilter, BackupTagFilter
from prime_backup.utils import collection_utils, db_utils, validation_utils

_T = TypeVar('_T')


class UnsupportedDatabaseOperation(PrimeBackupError):
	pass


# make type checker happy
def _list_it(seq: Sequence[_T]) -> List[_T]:
	if not isinstance(seq, list):
		seq = list(seq)
	return seq


def _int_or_0(value: Optional[int]) -> int:
	if value is None:
		return 0
	return int(value)


class DbSession:
	def __init__(self, session: Session, db_path: Path = None):
		self.session = session
		self.db_path = db_path

		# the limit in old sqlite (https://www.sqlite.org/limits.html#max_variable_number)
		self.__safe_var_limit = 999 - 20

	@classmethod
	def __check_support(cls, check_func: Callable[[], bool], msg: str):
		if not (is_supported := check_func()):
			from prime_backup import logger
			import sqlite3
			logger.get().warning(f'WARN: {msg}. SQLite version: {sqlite3.sqlite_version}')
		return is_supported

	@classmethod
	@functools.lru_cache
	def __supports_json_query(cls) -> bool:
		return cls.__check_support(db_utils.check_sqlite_json_query_support, 'SQLite backend does not support json query. Inefficient manual query is used as the fallback')

	@classmethod
	@functools.lru_cache
	def __supports_vacuum_into(cls) -> bool:
		return cls.__check_support(db_utils.check_sqlite_vacuum_into_support, 'SQLite backend does not support VACUUM INTO statement. Insecure manual file copy is used as the fallback')

	@classmethod
	def __validate_int_fields_range(cls, obj: schema.Base):
		from sqlalchemy import Integer, BigInteger

		for name, value in vars(obj).items():
			def bad_msg() -> str:
				return f'bad field {name} for {obj!r}'
			
			if not name.startswith('_') and isinstance(value, int):
				cls_value = getattr(type(obj), name, None)
				if isinstance(cls_value, Integer):
					validation_utils.validate_int32(value, bad_msg)
				elif isinstance(cls_value, BigInteger):
					validation_utils.validate_int64(value, bad_msg)

	# ========================= General Database Operations =========================

	def add(self, obj: schema.Base):
		self.session.add(obj)

	def expunge(self, obj: schema.Base):
		self.session.expunge(obj)

	def expunge_all(self):
		self.session.expunge_all()

	def flush(self):
		self.session.flush()

	def flush_and_expunge_all(self):
		self.flush()
		self.expunge_all()

	def commit(self):
		self.session.commit()

	@contextlib.contextmanager
	def no_auto_flush(self) -> ContextManager[None]:
		with self.session.no_autoflush:
			yield

	def vacuum(self, into_file: Optional[str] = None, allow_vacuum_into_fallback: bool = True):
		# https://www.sqlite.org/lang_vacuum.html
		if into_file is not None:
			if self.__supports_vacuum_into():
				self.session.execute(text('VACUUM INTO :into_file').bindparams(into_file=str(into_file)))
			elif allow_vacuum_into_fallback:
				self.session.execute(text('VACUUM'))
				self.session.commit()
				if self.db_path is None:
					raise RuntimeError('db_path undefined')
				shutil.copyfile(self.db_path, into_file)
			else:
				raise UnsupportedDatabaseOperation('current sqlite version {} does not support "VACUUM INTO" statement'.format(sqlite3.sqlite_version))
		else:
			self.session.execute(text('VACUUM'))

	# ==================================== DbMeta ====================================

	def get_db_meta(self) -> schema.DbMeta:
		meta: Optional[schema.DbMeta] = self.session.get(schema.DbMeta, db_constants.DB_MAGIC_INDEX)
		if meta is None:
			raise ValueError('None db meta')
		return meta

	# ===================================== Blob =====================================

	class CreateBlobKwargs(TypedDict):
		hash: str
		compress: str
		raw_size: int
		stored_size: int

	@classmethod
	def create_blob(cls, **kwargs: Unpack[CreateBlobKwargs]) -> schema.Blob:
		blob = schema.Blob(**kwargs)
		cls.__validate_int_fields_range(blob)
		return blob

	def create_and_add_blob(self, **kwargs: Unpack[CreateBlobKwargs]) -> schema.Blob:
		blob = self.create_blob(**kwargs)
		self.add(blob)
		return blob

	def get_blob_count(self) -> int:
		return _int_or_0(self.session.execute(select(func.count()).select_from(schema.Blob)).scalar_one())

	def get_blob_opt(self, h: str) -> Optional[schema.Blob]:
		return self.session.get(schema.Blob, h)

	def get_blob(self, h: str) -> schema.Blob:
		blob = self.get_blob_opt(h)
		if blob is None:
			raise BlobNotFound(h)
		return blob

	def get_blobs(self, hashes: List[str]) -> Dict[str, Optional[schema.Blob]]:
		"""
		:return: a dict, hash -> optional blob. All given hashes are in the dict
		"""
		result: Dict[str, Optional[schema.Blob]] = {h: None for h in hashes}
		for view in collection_utils.slicing_iterate(hashes, self.__safe_var_limit):
			for blob in self.session.execute(select(schema.Blob).where(schema.Blob.hash.in_(view))).scalars().all():
				result[blob.hash] = blob
		return result

	def list_blobs(self, limit: Optional[int] = None, offset: Optional[int] = None) -> List[schema.Blob]:
		s = select(schema.Blob)
		if limit is not None:
			s = s.limit(limit)
		if offset is not None:
			s = s.offset(offset)
		return _list_it(self.session.execute(s).scalars().all())

	def list_blob_with_hash_prefix(self, hash_prefix: str, limit: int) -> List[schema.Blob]:
		s = select(schema.Blob).where(schema.Blob.hash.startswith(hash_prefix, autoescape=True)).limit(limit)
		return _list_it(self.session.execute(s).scalars().all())

	def iterate_blob_batch(self, *, batch_size: int) -> Iterator[List[schema.Blob]]:
		limit, offset = batch_size, 0
		while True:
			blobs = self.list_blobs(limit=limit, offset=offset)
			if len(blobs) == 0:
				break
			yield blobs
			offset += limit

	def get_all_blob_hashes(self) -> List[str]:
		# TODO: don't load all blob into memory?
		return _list_it(self.session.execute(select(schema.Blob.hash)).scalars().all())

	def has_blob_with_size(self, raw_size: int) -> bool:
		q = self.session.query(schema.Blob).filter_by(raw_size=raw_size).exists()
		return self.session.query(q).scalar()

	def has_blob_with_size_batched(self, sizes: List[int]) -> Dict[int, bool]:
		result = {s: False for s in sizes}
		for view in collection_utils.slicing_iterate(sizes, self.__safe_var_limit):
			for size in self.session.execute(select(schema.Blob.raw_size).where(schema.Blob.raw_size.in_(view)).distinct()).scalars().all():
				result[size] = True
		return result

	def get_blob_stored_size_sum(self) -> int:
		return _int_or_0(self.session.execute(func.sum(schema.Blob.stored_size).select()).scalar_one())

	def get_blob_raw_size_sum(self) -> int:
		return _int_or_0(self.session.execute(func.sum(schema.Blob.raw_size).select()).scalar_one())

	def delete_blob(self, blob: schema.Blob):
		self.session.delete(blob)

	def delete_blobs(self, hashes: List[str]):
		for view in collection_utils.slicing_iterate(hashes, self.__safe_var_limit):
			self.session.execute(delete(schema.Blob).where(schema.Blob.hash.in_(view)))

	def filtered_orphan_blob_hashes(self, hashes: List[str]) -> List[str]:
		good_hashes = set()
		for view in collection_utils.slicing_iterate(hashes, self.__safe_var_limit):
			good_hashes.update(
				self.session.execute(
					select(schema.File.blob_hash).where(schema.File.blob_hash.in_(view)).distinct()
				).scalars().all()
			)
		return list(filter(lambda h: h not in good_hashes, hashes))

	# ===================================== File =====================================

	class CreateFileKwargs(TypedDict):
		fileset_id: NotRequired[int]
		path: str
		role: int
		mode: int
		content: Optional[bytes]
		blob_hash: NotRequired[Optional[str]]
		blob_compress: NotRequired[Optional[str]]
		blob_raw_size: NotRequired[Optional[int]]
		blob_stored_size: NotRequired[Optional[int]]
		uid: Optional[int]
		gid: Optional[int]
		mtime: Optional[int]

	@classmethod
	def create_file(cls, *, blob: Optional[schema.Blob] = None, **kwargs: Unpack[CreateFileKwargs]) -> schema.File:
		if blob is not None:
			kwargs.update(
				blob_hash=blob.hash,
				blob_compress=blob.compress,
				blob_raw_size=blob.raw_size,
				blob_stored_size=blob.stored_size,
			)
		file = schema.File(**kwargs)
		cls.__validate_int_fields_range(file)
		return file

	def get_file_in_fileset_opt(self, fileset_id: int, path: str) -> Optional[schema.File]:
		return self.session.get(schema.File, dict(fileset_id=fileset_id, path=path))

	def get_file_in_fileset(self, fileset_id: int, path: str) -> schema.File:
		file = self.get_file_in_fileset_opt(fileset_id, path)
		if file is None:
			raise FilesetFileNotFound(fileset_id, path)
		return file

	def get_file_in_backup_opt(self, backup_id: int, path: str) -> Optional[schema.File]:
		backup = self.get_backup(backup_id)

		file_delta = self.get_file_in_fileset_opt(backup.fileset_id_delta, path)
		if file_delta is not None:
			if file_delta.role not in [FileRole.delta_add.value, FileRole.delta_override.value]:
				return None
			return file_delta

		return self.get_file_in_fileset_opt(backup.fileset_id_base, path)

	def get_file_in_backup(self, backup_id: int, path: str) -> schema.File:
		file = self.get_file_in_backup_opt(backup_id, path)
		if file is None:
			raise BackupFileNotFound(backup_id, path)
		return file

	def get_file_object_count(self) -> int:
		return _int_or_0(self.session.execute(select(func.count()).select_from(schema.File)).scalar_one())

	def get_file_total_count(self) -> int:
		total_count = 0
		for col in [schema.Backup.fileset_id_base, schema.Backup.fileset_id_delta]:
			total_count += _int_or_0(self.session.execute(
			    func.sum(schema.Fileset.file_count).select().
			    join(schema.Backup, schema.Fileset.id == col)
			).scalar_one())
		return total_count

	def get_file_total_raw_size_sum(self) -> int:
		raw_size_sum = 0
		for col in [schema.Backup.fileset_id_base, schema.Backup.fileset_id_delta]:
			raw_size_sum += _int_or_0(self.session.execute(
			    func.sum(schema.Fileset.file_raw_size_sum).select().
			    join(schema.Backup, schema.Fileset.id == col)
			).scalar_one())
		return raw_size_sum

	def get_file_by_blob_hashes(self, hashes: List[str], *, limit: Optional[int] = None) -> List[schema.File]:
		hashes = collection_utils.deduplicated_list(hashes)
		result = []
		for view in collection_utils.slicing_iterate(hashes, self.__safe_var_limit):
			st = select(schema.File).where(schema.File.blob_hash.in_(view))
			if limit is not None:
				st = st.limit(max(0, limit - len(result)))
			result.extend(self.session.execute(st).scalars().all())
			if limit is not None and len(result) >= limit:
				break
		return result

	def get_file_count_by_blob_hashes(self, hashes: List[str]) -> int:
		cnt = 0
		for view in collection_utils.slicing_iterate(hashes, self.__safe_var_limit):
			cnt += _int_or_0(self.session.execute(
				select(func.count()).
				select_from(schema.File).
				where(schema.File.blob_hash.in_(view))
			).scalar_one())
		return cnt

	def list_files(self, limit: Optional[int] = None, offset: Optional[int] = None) -> List[schema.File]:
		s = select(schema.File)
		if limit is not None:
			s = s.limit(limit)
		if offset is not None:
			s = s.offset(offset)
		return _list_it(self.session.execute(s).scalars().all())

	def iterate_file_batch(self, *, batch_size: int = 5000) -> Iterator[List[schema.File]]:
		limit, offset = batch_size, 0
		while True:
			files = self.list_files(limit=limit, offset=offset)
			if len(files) == 0:
				break
			yield files
			offset += limit

	def delete_file(self, file: schema.File):
		self.session.delete(file)

	def has_file_with_hash(self, h: str):
		q = self.session.query(schema.File).filter_by(blob_hash=h).exists()
		exists = self.session.query(q).scalar()
		return exists

	def calc_file_stored_size_sum(self, fileset_id: int) -> int:
		return _int_or_0(self.session.execute(
			select(func.sum(schema.File.blob_stored_size)).
			where(schema.File.fileset_id == fileset_id)
		).scalar_one())

	# ==================================== Fileset ====================================

	class CreateFilesetKwargs(TypedDict):
		base_id: int
		file_object_count: int
		file_count: int
		file_raw_size_sum: int
		file_stored_size_sum: int

	def create_and_add_fileset(self, **kwargs: Unpack[CreateFilesetKwargs]) -> schema.Fileset:
		fileset = schema.Fileset(**kwargs)
		self.__validate_int_fields_range(fileset)
		self.add(fileset)
		return fileset

	def get_fileset_count(self) -> int:
		return _int_or_0(self.session.execute(select(func.count()).select_from(schema.Fileset)).scalar_one())

	def get_fileset_opt(self, fileset_id: int) -> Optional[schema.Fileset]:
		return self.session.get(schema.Fileset, fileset_id)

	def get_fileset(self, fileset_id: int) -> schema.Fileset:
		fileset = self.get_fileset_opt(fileset_id)
		if fileset is None:
			raise FilesetNotFound(fileset_id)
		return fileset

	def get_filesets(self, fileset_ids: List[int]) -> Dict[int, schema.Fileset]:
		"""
		:return: a dict, fileset id -> Fileset. All given ids are in the dict
		"""
		result: Dict[int, Optional[schema.Fileset]] = {fsid: None for fsid in fileset_ids}
		for view in collection_utils.slicing_iterate(fileset_ids, self.__safe_var_limit):
			for fileset in self.session.execute(select(schema.Fileset).where(schema.Fileset.id.in_(view))).scalars().all():
				result[fileset.id] = fileset
		for fileset_id, fileset in result.items():
			if fileset is None:
				raise FilesetNotFound(fileset_id)
		return result

	def get_fileset_associated_backup_count(self, fileset_id: int) -> int:
		return _int_or_0(self.session.execute(
			select(func.count()).
			select_from(schema.Backup).
			where(or_(
				schema.Backup.fileset_id_base == fileset_id,
				schema.Backup.fileset_id_delta == fileset_id,
			))
		).scalar_one())

	def get_fileset_associated_backup_ids(self, fileset_id: int, limit: Optional[int]) -> List[int]:
		if limit is not None and limit <= 0:
			return []
		return _list_it(self.session.execute(
			select(schema.Backup.id).
			where(or_(
				schema.Backup.fileset_id_base == fileset_id,
				schema.Backup.fileset_id_delta == fileset_id,
			)).
			order_by(desc(schema.Backup.id)).
			limit(limit)
		).scalars().all())

	def get_fileset_delta_file_object_count_sum(self, base_fileset_id: int) -> int:
		"""For those backups whose base fileset is the given one, sum up the file_object_count of their delta filesets"""
		delta_fileset_ids = self.session.execute(
			select(schema.Backup.fileset_id_delta).
			where(schema.Backup.fileset_id_base == base_fileset_id).
			distinct()
		).scalars().all()

		count_sum = 0
		for view in collection_utils.slicing_iterate(_list_it(delta_fileset_ids), self.__safe_var_limit):
			count_sum += _int_or_0(self.session.execute(
				select(func.sum(schema.Fileset.file_object_count)).
				select_from(schema.Fileset).
				where(schema.Fileset.id.in_(view))
			).scalar_one())
		return count_sum

	def get_last_n_base_fileset(self, limit: int) -> List[schema.Fileset]:
		return _list_it(self.session.execute(
			select(schema.Fileset).
			where(schema.Fileset.base_id == 0).
			order_by(desc(schema.Fileset.id)).
			limit(limit)
		).scalars().all())
	
	def get_fileset_files(self, fileset_id: Optional[int]) -> List[schema.File]:
		if fileset_id is None:
			return []
		return _list_it(self.session.execute(
			select(schema.File).
			where(schema.File.fileset_id == fileset_id)
		).scalars().all())

	def get_fileset_ids_by_blob_hashes(self, hashes: List[str]) -> List[int]:
		fileset_ids = set()
		for v_hashes in collection_utils.slicing_iterate(hashes, self.__safe_var_limit):
			fileset_ids.update(
				self.session.execute(
					select(schema.File.fileset_id).
					where(schema.File.blob_hash.in_(v_hashes)).
					distinct()
				).scalars().all()
			)
		return list(sorted(fileset_ids))

	def delete_fileset(self, fileset: schema.Fileset):
		self.session.delete(fileset)

	def list_fileset(self, limit: Optional[int] = None, offset: Optional[int] = None) -> List[schema.Fileset]:
		s = select(schema.Fileset)
		if limit is not None:
			s = s.limit(limit)
		if offset is not None:
			s = s.offset(offset)
		return _list_it(self.session.execute(s).scalars().all())

	def iterate_fileset_batch(self, *, batch_size: int = 1000) -> Iterator[List[schema.Fileset]]:
		limit, offset = batch_size, 0
		while True:
			filesets = self.list_fileset(limit=limit, offset=offset)
			if len(filesets) == 0:
				break
			yield filesets
			offset += limit

	# ==================================== Backup ====================================

	@classmethod
	def __needs_manual_backup_tag_filter(cls, backup_filter: Optional[BackupFilter]) -> bool:
		"""
		SQLite does not support json query, and the backup filter contains tag filter
		"""
		return not cls.__supports_json_query() and backup_filter is not None and len(backup_filter.tag_filters) > 0

	@classmethod
	def __manual_backup_tag_filter(cls, backup: schema.Backup, backup_filter: BackupFilter) -> bool:
		tags: BackupTagDict = backup.tags
		for tf in backup_filter.tag_filters:
			def check_one() -> bool:
				if tf.policy == BackupTagFilter.Policy.exists:
					return tf.name.name in tags
				elif tf.policy == BackupTagFilter.Policy.not_exists:
					return tf.name.name not in tags
				elif tf.policy == BackupTagFilter.Policy.equals:
					return tf.name.name in tags and tags[tf.name.name] == tf.value
				elif tf.policy == BackupTagFilter.Policy.not_equals:
					return tf.name.name not in tags or tags[tf.name.name] != tf.value
				elif tf.policy == BackupTagFilter.Policy.exists_and_not_equals:
					return tf.name.name in tags and tags[tf.name.name] != tf.value
				else:
					raise ValueError(tf.policy)
			if not check_one():
				return False
		return True

	@classmethod
	def __sql_backup_tag_filter(cls, s: Select[_T], backup_filter: BackupFilter) -> Select[_T]:
		for tf in backup_filter.tag_filters:
			element = schema.Backup.tags[tf.name.name]
			if tf.policy == BackupTagFilter.Policy.exists:
				s = s.filter(element != JSON.NULL)
			elif tf.policy == BackupTagFilter.Policy.not_exists:
				s = s.filter(element == JSON.NULL)
			elif tf.policy in [BackupTagFilter.Policy.equals, BackupTagFilter.Policy.not_equals, BackupTagFilter.Policy.exists_and_not_equals]:
				value_type = tf.name.value.type
				if value_type == bool:
					js_value, value = element.as_boolean(), bool(tf.value)
				elif value_type == str:
					js_value, value = element.as_string(), str(tf.value)
				elif value_type == float:
					js_value, value = element.as_float(), float(tf.value)
				elif value_type == int:
					js_value, value = element.as_integer(), int(tf.value)
				else:
					raise TypeError(value_type)

				if tf.policy == BackupTagFilter.Policy.equals:
					filter_ = js_value == value
				elif tf.policy == BackupTagFilter.Policy.not_equals:
					filter_ = (js_value != value) | (element == JSON.NULL)
				elif tf.policy == BackupTagFilter.Policy.exists_and_not_equals:
					filter_ = js_value != value
				else:
					raise ValueError(tf.policy)

				s = s.filter(filter_)
			else:
				raise ValueError(tf.policy)
		return s

	@classmethod
	def __apply_backup_filter(cls, s: Select[_T], backup_filter: BackupFilter) -> Select[_T]:
		if backup_filter.id_start is not None:
			s = s.where(schema.Backup.id >= backup_filter.id_start)
		if backup_filter.id_end is not None:
			s = s.where(schema.Backup.id <= backup_filter.id_end)
		if backup_filter.creator is not None:
			s = s.filter_by(creator=str(backup_filter.creator))
		if backup_filter.timestamp_us_start is not None:
			s = s.where(schema.Backup.timestamp >= backup_filter.timestamp_us_start)
		if backup_filter.timestamp_us_end is not None:
			s = s.where(schema.Backup.timestamp <= backup_filter.timestamp_us_end)
		if cls.__supports_json_query():
			s = cls.__sql_backup_tag_filter(s, backup_filter)
		return s

	class CreateBackupKwargs(TypedDict):
		timestamp: NotRequired[int]
		creator: str
		comment: str
		targets: List[str]
		tags: BackupTagDict
		fileset_id_base: NotRequired[int]
		fileset_id_delta: NotRequired[int]
		file_count: NotRequired[int]
		file_raw_size_sum: NotRequired[int]
		file_stored_size_sum: NotRequired[int]

	@classmethod
	def create_backup(cls, **kwargs: Unpack[CreateBackupKwargs]) -> schema.Backup:
		"""
		Notes: the backup id is not generated yet. Invoke :meth:`flush` to generate the backup id
		"""
		if 'timestamp' not in kwargs:
			kwargs['timestamp'] = time.time_ns() // 1000

		if kwargs.get('tags', {}).get('pre_restore_backup') is not None:
			from prime_backup import logger
			logger.get().warning('Backup tag "pre_restore_backup" is not used anymore, use tag "temporary" instead')

		backup = schema.Backup(**kwargs)
		cls.__validate_int_fields_range(backup)
		return backup

	def get_backup_count(self, backup_filter: Optional[BackupFilter] = None) -> int:
		if self.__needs_manual_backup_tag_filter(backup_filter):
			s = self.__apply_backup_filter(select(schema.Backup), backup_filter)
			backups = [backup for backup in self.session.execute(s).scalars().all() if self.__manual_backup_tag_filter(backup, backup_filter)]
			return len(backups)
		else:
			s = select(func.count()).select_from(schema.Backup)
			if backup_filter is not None:
				s = self.__apply_backup_filter(s, backup_filter)
			return _int_or_0(self.session.execute(s).scalar_one())

	def get_backup_opt(self, backup_id: int) -> Optional[schema.Backup]:
		return self.session.get(schema.Backup, backup_id)

	def get_backup(self, backup_id: int) -> schema.Backup:
		backup = self.get_backup_opt(backup_id)
		if backup is None:
			raise BackupNotFound(backup_id)
		return backup

	@overload
	def get_backup_files(self, backup_id: int) -> List[schema.File]: ...
	@overload
	def get_backup_files(self, backup: schema.Backup) -> List[schema.File]: ...

	def get_backup_files(self, backup_or_backup_id: Union[int, schema.Backup]) -> List[schema.File]:
		if isinstance(backup_or_backup_id, schema.Backup):
			backup = backup_or_backup_id
		elif isinstance(backup_or_backup_id, int):
			backup = self.get_backup(backup_or_backup_id)
		else:
			raise TypeError(type(backup_or_backup_id))

		files_base = self.get_fileset_files(backup.fileset_id_base)
		files_delta = self.get_fileset_files(backup.fileset_id_delta)
		if len(files_delta) == 0:
			return files_base

		path_to_file = {file.path: file for file in files_base}
		for file in files_delta:
			if file.role in [FileRole.delta_add.value, FileRole.delta_override.value]:
				path_to_file[file.path] = file
			elif file.role == FileRole.delta_remove:
				path_to_file.pop(file.path, None)

		return list(path_to_file.values())

	def get_backups(self, backup_ids: List[int]) -> Dict[int, schema.Backup]:
		"""
		:return: a dict, backup id -> Backup. All given ids are in the dict
		"""
		result: Dict[int, Optional[schema.Backup]] = {bid: None for bid in backup_ids}
		for view in collection_utils.slicing_iterate(backup_ids, self.__safe_var_limit):
			for backup in self.session.execute(select(schema.Backup).where(schema.Backup.id.in_(view))).scalars().all():
				result[backup.id] = backup
		for backup_id, backup in result.items():
			if backup is None:
				raise BackupNotFound(backup_id)
		return result

	def get_backup_ids_by_fileset_ids(self, fileset_ids: List[int]) -> List[int]:
		backup_ids = set()
		for v_fs_ids in collection_utils.slicing_iterate(fileset_ids, self.__safe_var_limit):
			backup_ids.update(
				self.session.execute(
					select(schema.Backup.id).
					where(or_(
						schema.Backup.fileset_id_base.in_(v_fs_ids),
						schema.Backup.fileset_id_delta.in_(v_fs_ids),
					))
				).scalars().all()
			)
		return list(sorted(backup_ids))

	def get_backup_ids_by_blob_hashes(self, hashes: List[str]) -> List[int]:
		fileset_ids = self.get_fileset_ids_by_blob_hashes(hashes)
		return self.get_backup_ids_by_fileset_ids(fileset_ids)

	def get_last_backup(self) -> Optional[schema.Backup]:
		s = select(schema.Backup).order_by(desc(schema.Backup.id)).limit(1)
		backups = _list_it(self.session.execute(s).scalars().all())
		return backups[0] if backups else None

	def list_backup(self, backup_filter: Optional[BackupFilter] = None, limit: Optional[int] = None, offset: Optional[int] = None) -> List[schema.Backup]:
		s = select(schema.Backup)
		if backup_filter is not None:
			s = self.__apply_backup_filter(s, backup_filter)
		s = s.order_by(desc(schema.Backup.timestamp), desc(schema.Backup.id))

		if self.__needs_manual_backup_tag_filter(backup_filter):
			backups = [backup for backup in self.session.execute(s).scalars().all() if self.__manual_backup_tag_filter(backup, backup_filter)]
			if offset is not None:
				backups = backups[offset:]
			if limit is not None:
				backups = backups[:limit]
			return backups
		else:
			if offset is not None:
				s = s.offset(offset)
			if limit is not None:
				s = s.limit(limit)
			return _list_it(self.session.execute(s).scalars().all())

	def iterate_backup_batch(self, *, batch_size: int = 1000) -> Iterator[List[schema.Backup]]:
		limit, offset = batch_size, 0
		while True:
			backups = self.list_backup(limit=limit, offset=offset)
			if len(backups) == 0:
				break
			yield backups
			offset += limit

	def delete_backup(self, backup: schema.Backup):
		self.session.delete(backup)
