import time
from typing import Optional

from sqlalchemy.orm import Session

from xbackup.db import schema, db_logger


class DbSession:
	def __init__(self, session: Session):
		self.session = session
		self.logger = db_logger.get_logger()

	def get_blob(self, h: str) -> Optional[schema.Blob]:
		self.logger.info('hash: %s', h)
		blob = self.session.get(schema.Blob, h)
		self.logger.info('result: %s', blob)
		return blob

	def create_blob(self, **kwargs) -> schema.Blob:
		blob = schema.Blob(**kwargs)
		self.session.add(blob)
		self.logger.info('created: %s', blob)
		return blob

	def create_file(self, **kwargs) -> schema.File:
		file = schema.File(**kwargs)
		self.session.add(file)
		self.logger.info('created: %s', file)
		return file

	def create_backup(self, **kwargs) -> schema.Backup:
		kwargs['timestamp'] = int(time.time() * 1000)
		backup = schema.Backup(**kwargs)
		self.session.add(backup)
		self.session.flush()  # so the backup id populates
		self.logger.info('created: %s', backup)
		return backup

	def get_backup(self, backup_id: int) -> Optional[schema.Backup]:
		self.logger.info('backup_id: %s', backup_id)
		backup = self.session.get(schema.Backup, backup_id)
		self.logger.info('result: %s', backup)
		return backup
