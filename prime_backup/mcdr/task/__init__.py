import enum
from abc import ABC, abstractmethod
from typing import Union

from mcdreforged.api.all import RTextBase, CommandSource

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

		from prime_backup import logger
		from prime_backup.config.config import Config
		self.logger = logger.get()
		self.config = Config.get()

	def get_name_text(self) -> RTextBase:
		return self.tr('name')

	@property
	@abstractmethod
	def name(self) -> str:
		...

	@abstractmethod
	def run(self) -> None:
		...

	def on_event(self, event: TaskEvent):
		pass

	# ==================================== Utils ====================================

	def reply(self, msg: Union[str, RTextBase], *, with_prefix: bool = True):
		mcdr_utils.reply_message(self.source, msg, with_prefix=with_prefix)

	def broadcast(self, msg: Union[str, RTextBase], *, with_prefix: bool = True):
		mcdr_utils.broadcast_message(self.source, msg, with_prefix=with_prefix)


class OperationTask(Task, ABC):
	pass


class ReaderTask(Task, ABC):
	pass


class ImmediateTask(Task, ABC):
	pass

