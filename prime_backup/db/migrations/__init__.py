from sqlalchemy import Engine
from sqlalchemy.orm import Session

from prime_backup import logger


class MigrationImplBase:
	def __init__(self, engine: Engine, session: Session):
		self.logger = logger.get()
		self.engine = engine
		self.session = session
