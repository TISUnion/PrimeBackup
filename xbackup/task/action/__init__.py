from abc import ABC, abstractmethod
from typing import Any


class Action(ABC):
	def __init__(self):
		from xbackup import logger
		from xbackup.config.config import Config
		self.logger = logger.get()
		self.config = Config.get()

	@abstractmethod
	def run(self) -> Any:
		...
