import time
from typing import Optional, Sequence, Dict
from typing import TypeVar, List

from sqlalchemy import select, delete, desc, func, Select
from sqlalchemy.orm import Session

from prime_backup.db import schema, db_logger
from prime_backup.exceptions import BackupNotFound
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.utils import collection_utils

_T = TypeVar('_T')


# make type checker happy
def _list_it(seq: Sequence[_T]) -> List[_T]:
	if not isinstance(seq, list):
		seq = list(seq)
	return seq


class DbSession:
	def __init__(self, session: Session):
		self.session = session
		self.logger = db_logger.get_logger()

		# the limit in old sqlite (https://www.sqlite.org/limits.html#max_variable_number)
		self.__safe_var_limit = 999 - 20

	# ========================= General Database Operations =========================

	def flush(self):
		self.session.flush()

	# ===================================== Blob =====================================

	def create_blob(self, **kwargs) -> schema.Blob:
		blob = schema.Blob(**kwargs)
		self.session.add(blob)
		return blob

	def get_blob(self, h: str) -> Optional[schema.Blob]:
		return self.session.get(schema.Blob, h)

	def get_blobs(self, hashes: List[str]) -> Dict[str, schema.Blob]:
		result = {h: None for h in hashes}
		for view in collection_utils.slicing_iterate(hashes, self.__safe_var_limit):
			for blob in self.session.execute(select(schema.Blob).where(schema.Blob.hash.in_(view))).scalars().all():
				result[blob.hash] = blob
		return result

	def get_all_blobs(self) -> List[schema.Blob]:
		return _list_it(self.session.execute(select(schema.Blob)).scalars().all())

	def get_all_blob_hashes(self) -> List[str]:
		return _list_it(self.session.execute(select(schema.Blob.hash)).scalars().all())

	def has_blob_with_size(self, size: int) -> bool:
		q = self.session.query(schema.Blob).filter_by(size=size).exists()
		return self.session.query(q).scalar()

	def has_blob_with_size_batched(self, sizes: List[int]) -> Dict[int, bool]:
		result = {s: False for s in sizes}
		for view in collection_utils.slicing_iterate(sizes, self.__safe_var_limit):
			for size in self.session.execute(select(schema.Blob.size).where(schema.Blob.size.in_(view)).distinct()).scalars().all():
				result[size] = True
		return result

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

	def create_file(self, **kwargs) -> schema.File:
		file = schema.File(**kwargs)
		self.session.add(file)
		return file

	def delete_file(self, file: schema.File):
		self.session.delete(file)

	def has_file_with_hash(self, h: str):
		q = self.session.query(schema.File).filter_by(blob_hash=h).exists()
		exists = self.session.query(q).scalar()
		return exists

	# ==================================== Backup ====================================

	def create_backup(self, **kwargs) -> schema.Backup:
		kwargs['timestamp'] = int(time.time() * 1e9)
		backup = schema.Backup(**kwargs)
		self.session.add(backup)
		self.session.flush()  # so the backup id populates
		return backup

	def get_backup_opt(self, backup_id: int) -> Optional[schema.Backup]:
		return self.session.get(schema.Backup, backup_id)

	def get_backup(self, backup_id: int) -> schema.Backup:
		backup = self.get_backup_opt(backup_id)
		if backup is None:
			raise BackupNotFound(backup_id)
		return backup

	@staticmethod
	def __apply_backup_filter(s: Select[_T], backup_filter: BackupFilter) -> Select[_T]:
		if backup_filter.author is not None:
			s = s.filter_by(author=str(backup_filter.author))
		if backup_filter.timestamp_lower is not None:
			s = s.where(schema.Backup.timestamp >= backup_filter.timestamp_lower)
		if backup_filter.timestamp_upper is not None:
			s = s.where(schema.Backup.timestamp <= backup_filter.timestamp_upper)
		if backup_filter.hidden is not None:
			s = s.filter_by(hidden=backup_filter.hidden)
		return s

	def get_backup_count(self, backup_filter: Optional[BackupFilter] = None) -> int:
		s = select(func.count()).select_from(schema.Backup)
		if backup_filter is not None:
			s = self.__apply_backup_filter(s, backup_filter)
		return self.session.execute(s).scalar()

	def list_backup(self, backup_filter: Optional[BackupFilter] = None, limit: Optional[int] = None, offset: Optional[int] = None) -> List[schema.Backup]:
		s = select(schema.Backup)
		if backup_filter is not None:
			s = self.__apply_backup_filter(s, backup_filter)
		s = s.order_by(desc(schema.Backup.timestamp))
		if offset is not None:
			s = s.offset(offset)
		if limit is not None:
			s = s.limit(limit)
		return _list_it(self.session.execute(s).scalars().all())

	def delete_backup(self, backup: schema.Backup):
		self.session.delete(backup)
