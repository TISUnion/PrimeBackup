import functools
import time
from typing import Optional, Sequence, Dict
from typing import TypeVar, List

from sqlalchemy import select, delete
from sqlalchemy.orm import Session

from xbackup.db import schema, db_logger
from xbackup.utils import collection_utils

_T = TypeVar('_T')


# make type checker happy
def _list_it(seq: Sequence[_T]) -> List[_T]:
	if not isinstance(seq, list):
		seq = list(seq)
	return seq


class DbSession:
	def __init__(self, session: Session, max_variable_number: int = 999):
		self.session = session
		self.logger = db_logger.get_logger()
		self.__max_variable_number = max_variable_number

	@functools.cached_property
	def __safe_var_limit(self) -> int:
		return max(10, self.__max_variable_number - 100)

	def flush(self):
		self.session.flush()

	def create_blob(self, **kwargs) -> schema.Blob:
		blob = schema.Blob(**kwargs)
		self.session.add(blob)
		# self.logger.info('created: %s', blob)
		return blob

	def get_blob(self, h: str) -> Optional[schema.Blob]:
		return self.session.get(schema.Blob, h)

	def get_blobs(self, hashes: List[str]) -> Dict[str, schema.Blob]:
		result = {}
		for view in collection_utils.slicing_iterate(hashes, self.__safe_var_limit):
			for blob in self.session.execute(select(schema.Blob).where(schema.Blob.hash.in_(view))).scalars().all():
				result[blob.hash] = blob
		return result

	def get_all_blobs(self) -> List[schema.Blob]:
		return _list_it(self.session.execute(select(schema.Blob)).scalars().all())

	def get_all_blob_hashes(self) -> List[str]:
		return _list_it(self.session.execute(select(schema.Blob.hash)).scalars().all())

	def has_blob_with_size(self, size: int) -> bool:
		# self.logger.info('size: %s', size)
		q = self.session.query(schema.Blob).filter_by(size=size).exists()
		exists = self.session.query(q).scalar()
		# self.logger.info('result: %s', exists)
		return exists

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

	def delete_blob(self, blob: schema.Blob):
		# self.logger.info('hash: %s', blob.hash)
		self.session.delete(blob)

	def create_file(self, **kwargs) -> schema.File:
		file = schema.File(**kwargs)
		self.session.add(file)
		# self.logger.info('created: %s', file)
		return file

	def delete_file(self, file: schema.File):
		# self.logger.info('backup_id: %s, path: %s', file.backup_id, file.path)
		self.session.delete(file)

	def create_backup(self, **kwargs) -> schema.Backup:
		kwargs['timestamp'] = int(time.time() * 1e9)
		backup = schema.Backup(**kwargs)
		self.session.add(backup)
		self.session.flush()  # so the backup id populates
		# self.logger.info('created: %s', backup)
		return backup

	def get_backup(self, backup_id: int) -> Optional[schema.Backup]:
		# self.logger.info('backup_id: %s', backup_id)
		backup = self.session.get(schema.Backup, backup_id)
		# self.logger.info('result: %s', backup)
		return backup

	def delete_backup(self, backup: schema.Backup):
		# self.logger.info('backup_id: %s', backup.id)
		self.session.delete(backup)

	def has_file_with_hash(self, h: str):
		q = self.session.query(schema.File).filter_by(blob_hash=h).exists()
		exists = self.session.query(q).scalar()
		return exists
