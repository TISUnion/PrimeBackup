import contextlib
import time
from typing import Optional, Sequence, Dict, ContextManager, Iterator
from typing import TypeVar, List

from sqlalchemy import select, delete, desc, func, Select, JSON, text
from sqlalchemy.orm import Session

from prime_backup.db import schema, db_logger, db_constants
from prime_backup.exceptions import BackupNotFound, BackupFileNotFound
from prime_backup.types.backup_filter import BackupFilter, BackupTagFilter
from prime_backup.utils import collection_utils

T = TypeVar('T')


# make type checker happy
def _list_it(seq: Sequence[T]) -> List[T]:
	if not isinstance(seq, list):
		seq = list(seq)
	return seq


def _int_or_0(value: Optional[int]) -> int:
	if value is None:
		return 0
	return int(value)


class DbSession:
	def __init__(self, session: Session):
		self.session = session
		self.logger = db_logger.get()

		# the limit in old sqlite (https://www.sqlite.org/limits.html#max_variable_number)
		self.__safe_var_limit = 999 - 20

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

	def vacuum(self, into_file: Optional[str] = None):
		# https://www.sqlite.org/lang_vacuum.html
		if into_file is not None:
			self.session.execute(text(f"VACUUM INTO '{into_file}'"))
		else:
			self.session.execute(text('VACUUM'))

	# ==================================== DbMeta ====================================

	def get_db_meta(self) -> schema.DbMeta:
		meta: Optional[schema.DbMeta] = self.session.get(schema.DbMeta, db_constants.DB_MAGIC_INDEX)
		if meta is None:
			raise ValueError('None db meta')
		return meta

	# ===================================== Blob =====================================

	def create_blob(self, **kwargs) -> schema.Blob:
		blob = schema.Blob(**kwargs)
		self.session.add(blob)
		return blob

	def get_blob_count(self) -> int:
		return _int_or_0(self.session.execute(select(func.count()).select_from(schema.Blob)).scalar_one())

	def get_blob_opt(self, h: str) -> Optional[schema.Blob]:
		return self.session.get(schema.Blob, h)

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

	def iterate_blob_batch(self, *, batch_size: int = 3000) -> Iterator[List[schema.Blob]]:
		limit, offset = batch_size, 0
		while True:
			blobs = self.list_blobs(limit=limit, offset=offset)
			if len(blobs) == 0:
				break
			yield blobs

	def get_all_blob_hashes(self) -> List[str]:
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

	def create_file(self, *, add_to_session: bool = True, blob: Optional[schema.Blob] = None, **kwargs) -> schema.File:
		if blob is not None:
			kwargs |= dict(
				blob_hash=blob.hash,
				blob_compress=blob.compress,
				blob_raw_size=blob.raw_size,
				blob_stored_size=blob.stored_size,
			)
		file = schema.File(**kwargs)
		if add_to_session:
			self.session.add(file)
		return file

	def get_file_count(self) -> int:
		return _int_or_0(self.session.execute(select(func.count()).select_from(schema.File)).scalar_one())

	def get_file_opt(self, backup_id: int, path: str) -> Optional[schema.File]:
		return self.session.get(schema.File, dict(backup_id=backup_id, path=path))

	def get_file(self, backup_id: int, path: str) -> schema.File:
		file = self.get_file_opt(backup_id, path)
		if file is None:
			raise BackupFileNotFound(backup_id, path)
		return file

	def get_file_raw_size_sum(self) -> int:
		return _int_or_0(self.session.execute(func.sum(schema.File.blob_raw_size).select()).scalar_one())

	def get_file_by_blob_hashes(self, hashes: List[str]) -> List[schema.File]:
		hashes = collection_utils.deduplicated_list(hashes)
		result = []
		for view in collection_utils.slicing_iterate(hashes, self.__safe_var_limit):
			result.extend(self.session.execute(
				select(schema.File).
				where(schema.File.blob_hash.in_(view))
			).scalars().all())
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

	def iterate_file_batch(self, *, batch_size: int = 3000) -> Iterator[List[schema.File]]:
		limit, offset = batch_size, 0
		while True:
			files = self.list_files(limit=limit, offset=offset)
			if len(files) == 0:
				break
			yield files

	def delete_file(self, file: schema.File):
		self.session.delete(file)

	def has_file_with_hash(self, h: str):
		q = self.session.query(schema.File).filter_by(blob_hash=h).exists()
		exists = self.session.query(q).scalar()
		return exists

	# ==================================== Backup ====================================

	def create_backup(self, **kwargs) -> schema.Backup:
		"""
		Notes: the backup id is not generated yet. Invoke :meth:`flush` to generate the backup id
		"""
		if 'timestamp' not in kwargs:
			kwargs['timestamp'] = time.time_ns()
		backup = schema.Backup(**kwargs)
		self.session.add(backup)
		return backup

	def get_backup_count(self, backup_filter: Optional[BackupFilter] = None) -> int:
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

	def get_backup_ids_by_blob_hashes(self, hashes: List[str]) -> List[int]:
		backup_ids = set()
		for view in collection_utils.slicing_iterate(hashes, self.__safe_var_limit):
			backup_ids.update(
				self.session.execute(
					select(schema.File.backup_id).
					where(schema.File.blob_hash.in_(view)).
					distinct()
				).scalars().all()
			)
		return list(sorted(backup_ids))

	@staticmethod
	def __apply_backup_filter(s: Select[T], backup_filter: BackupFilter) -> Select[T]:
		if backup_filter.id_start is not None:
			s = s.where(schema.Backup.id >= backup_filter.id_start)
		if backup_filter.id_end is not None:
			s = s.where(schema.Backup.id <= backup_filter.id_end)
		if backup_filter.creator is not None:
			s = s.filter_by(creator=str(backup_filter.creator))
		if backup_filter.timestamp_start is not None:
			s = s.where(schema.Backup.timestamp >= backup_filter.timestamp_start)
		if backup_filter.timestamp_end is not None:
			s = s.where(schema.Backup.timestamp <= backup_filter.timestamp_end)
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

	def list_backup(self, backup_filter: Optional[BackupFilter] = None, limit: Optional[int] = None, offset: Optional[int] = None) -> List[schema.Backup]:
		s = select(schema.Backup)
		if backup_filter is not None:
			s = self.__apply_backup_filter(s, backup_filter)
		s = s.order_by(desc(schema.Backup.id))
		if offset is not None:
			s = s.offset(offset)
		if limit is not None:
			s = s.limit(limit)
		return _list_it(self.session.execute(s).scalars().all())

	def delete_backup(self, backup: schema.Backup):
		self.session.delete(backup)
