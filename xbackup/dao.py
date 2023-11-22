import contextlib
import datetime
from pathlib import Path
from typing import Optional, ContextManager

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from xbackup import schema


class DAO:
	__db_path: Optional[Path] = None

	@classmethod
	def set_db(cls, db_path: Path):
		cls.__db_path = db_path

	@classmethod
	@contextlib.contextmanager
	def open_session(cls) -> ContextManager[Session]:
		engine = create_engine('sqlite:///' + str(cls.__db_path))
		schema.Base.metadata.create_all(engine)
		with Session(engine) as session:
			yield session

	@classmethod
	def create_backup(cls, comment: str) -> schema.Backup:
		with cls.open_session() as session:
			backup = schema.Backup(comment=comment, date=datetime.datetime.now())
			session.add(datetime)
		return backup

	@classmethod
	def get_blob(cls, h: str) -> Optional[schema.Blob]:
		with cls.open_session() as session:
			return session.execute(select(schema.Blob).filter_by(hash=h)).one_or_none()
