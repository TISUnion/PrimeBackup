import enum
from abc import ABC, abstractmethod

from mcdreforged.api.all import *

from prime_backup.utils import mcdr_utils


class TaskEvent(enum.Enum):
	plugin_unload = enum.auto()
	world_save_done = enum.auto()
	operation_confirmed = enum.auto()
	operation_aborted = enum.auto()


class Task(mcdr_utils.TranslationContext, ABC):
	def __init__(self, source: CommandSource):
		super().__init__(f'task.{self.name}')
		self.source = source
		self.server = source.get_server()

	def get_name_text(self) -> RTextBase:
		return self.tr('name').set_color(RColor.aqua)

	def is_abort_able(self) -> bool:
		return False

	@property
	@abstractmethod
	def name(self) -> str:
		...

	@abstractmethod
	def run(self) -> None:
		...

	def on_event(self, event: TaskEvent):
		pass
