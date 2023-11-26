from abc import abstractmethod, ABC
from typing import Any

from xbackup import logger
from xbackup.task.event import TaskEvent


class Task(ABC):
	def __init__(self):
		self.logger = logger.get()

	@abstractmethod
	def run(self) -> Any:
		...

	def on_event(self, event: TaskEvent):
		pass
