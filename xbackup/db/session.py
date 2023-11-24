import time
from typing import Optional

from sqlalchemy.orm import Session

from xbackup.db import schema, db_logger


class DbSession:
	def __init__(self, session: Session):
		self.session = session
		self.logger = db_logger.get_logger()

	def create_blob(self, **kwargs) -> schema.Blob:
		blob = schema.Blob(**kwargs)
		self.session.add(blob)
		# self.logger.info('created: %s', blob)
		return blob

	def get_blob(self, h: str) -> Optional[schema.Blob]:
		# self.logger.info('hash: %s', h)
		blob = self.session.get(schema.Blob, h)
		# self.logger.info('result: %s', blob)
		return blob

	def has_blob_with_size(self, size: int) -> bool:
		# self.logger.info('size: %s', size)
		q = self.session.query(schema.Blob).filter_by(size=size).exists()
		exists = self.session.query(q).scalar()
		# self.logger.info('result: %s', exists)
		return exists

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
