import enum
from abc import abstractmethod, ABC

from xbackup import logger
from xbackup.config.config import Config


class TaskEvent(enum.Enum):
	plugin_unload = enum.auto()
	world_save_done = enum.auto()
	operation_confirmed = enum.auto()
	operation_cancelled = enum.auto()
	operation_aborted = enum.auto()


class Task(ABC):
	def __init__(self):
		self.logger = logger.get()
		self.config = Config.get()

	@abstractmethod
	def run(self):
		...

	def on_event(self, event: TaskEvent):
		pass
