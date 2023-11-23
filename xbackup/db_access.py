import contextlib
import datetime
import logging
from typing import Optional, ContextManager

from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import Session

from xbackup import schema
from xbackup.config.config import Config

_logger: Optional[logging.Logger] = None


class DbAccess:
	__engine: Optional[Engine] = None

	@classmethod
	def init(cls):
		db_dir = Config.get().storage_path
		db_dir.mkdir(parents=True, exist_ok=True)
		db_path = db_dir / 'xbackup.db'
		cls.__engine = create_engine('sqlite:///' + str(db_path))
		schema.Base.metadata.create_all(cls.__engine)

		global _logger
		# TODO: concurrency write proof
		logs_dir = Config.get().storage_path / 'logs'
		logs_dir.mkdir(parents=True, exist_ok=True)
		_logger = logging.Logger('xbackup-db')
		_logger.setLevel(logging.DEBUG)  # TODO: configure-able
		handler = logging.FileHandler(logs_dir / 'db.log', encoding='utf8')
		handler.setFormatter(logging.Formatter('[%(asctime)s %(levelname)s] [%(funcName)s] %(message)s'))
		_logger.addHandler(handler)

	@classmethod
	@contextlib.contextmanager
	def open_session(cls) -> ContextManager['DbSession']:
		with Session(cls.__engine) as session, session.begin():
			yield DbSession(session)

	@classmethod
	def get_logger(cls) -> logging.Logger:
		return _logger


class DbSession:
	def __init__(self, session: Session):
		self.session = session
		self.logger = _logger

	def create_backup(self, comment: str) -> schema.Backup:
		self.logger.info('comment: %s', comment)
		backup = schema.Backup(comment=comment, date=datetime.datetime.now())
		self.session.add(backup)
		self.session.flush()  # so the backup id populates
		self.logger.info('result: %s', backup)
		return backup

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
