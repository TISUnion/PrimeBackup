import enum
from abc import ABC, abstractmethod


class TaskEvent(enum.Enum):
	shutdown = enum.auto()
	world_save_done = enum.auto()
	operation_confirmed = enum.auto()
	operation_cancelled = enum.auto()
	operation_aborted = enum.auto()


class Task(ABC):
	def __init__(self):
		from prime_backup import logger
		from prime_backup.config.config import Config
		self.logger = logger.get()
		self.config = Config.get()

	@abstractmethod
	def run(self) -> None:
		...

	def on_event(self, event: TaskEvent):
		pass
