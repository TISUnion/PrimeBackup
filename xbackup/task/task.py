from abc import abstractmethod, ABC
from typing import Any

from xbackup import logger


class Task(ABC):
	def __init__(self):
		self.logger = logger.get()

	@abstractmethod
	def run(self) -> Any:
		...
