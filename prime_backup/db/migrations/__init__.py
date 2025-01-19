import os
from abc import abstractmethod, ABC
from pathlib import Path

from sqlalchemy import Engine
from sqlalchemy.orm import Session
from typing_extensions import final

from prime_backup import logger
from prime_backup.utils.temp_file_store import TempFileStore


class MigrationImplBase(ABC):
	def __init__(self, engine: Engine, temp_dir: Path, session: Session):
		self.logger = logger.get()
		self.engine = engine
		self.temp_file_store = TempFileStore(temp_dir / f'migration_{os.getpid()}')
		self.session = session

	@final
	def migrate(self):
		with self.temp_file_store:
			self._migrate()

	@abstractmethod
	def _migrate(self):
		raise NotImplementedError()
