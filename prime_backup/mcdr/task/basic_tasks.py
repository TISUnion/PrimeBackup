import threading
from abc import ABC
from typing import Union, Optional

from mcdreforged.api.all import *
from typing_extensions import final

from prime_backup.mcdr.task import Task, TaskEvent
from prime_backup.mcdr.task.task_utils import ConfirmHelper, ConfirmResult
from prime_backup.types.units import Duration
from prime_backup.utils import mcdr_utils
from prime_backup.utils.waitable_value import WaitableValue


class _BasicTask(Task, ABC):
	def __init__(self, source: CommandSource):
		super().__init__(source)

		from prime_backup import logger
		from prime_backup.config.config import Config
		self.logger = logger.get()
		self.config = Config.get()

		self.aborted_event = threading.Event()
		self.plugin_unloaded_event = threading.Event()
		self.is_waiting_confirm = False
		self._confirm_helper = ConfirmHelper()

	# ==================================== Overrides ====================================

	def is_abort_able(self) -> bool:
		return self.is_waiting_confirm

	def on_event(self, event: TaskEvent):
		self._confirm_helper.on_event(event)
		if event in [TaskEvent.plugin_unload, TaskEvent.operation_aborted]:
			if event == TaskEvent.plugin_unload:
				self.plugin_unloaded_event.set()
			self.aborted_event.set()

	# ==================================== Utils ====================================

	def wait_confirm(self, confirm_target_text: RTextBase, time_wait: Optional[Duration] = None) -> WaitableValue[ConfirmResult]:
		if time_wait is None:
			time_wait = self.config.command.confirm_time_wait

		self.is_waiting_confirm = True
		try:
			return self._confirm_helper.wait_confirm(confirm_target_text, time_wait)
		finally:
			self.is_waiting_confirm = False

	def reply(self, msg: Union[str, RTextBase], *, with_prefix: bool = True):
		mcdr_utils.reply_message(self.source, msg, with_prefix=with_prefix)

	@classmethod
	def broadcast(cls, msg: Union[str, RTextBase], *, with_prefix: bool = True):
		mcdr_utils.broadcast_message(msg, with_prefix=with_prefix)


class OperationTask(_BasicTask, ABC):
	pass


class ReaderTask(_BasicTask, ABC):
	pass


class ImmediateTask(_BasicTask, ABC):
	@final
	def is_abort_able(self) -> bool:
		return super().is_abort_able()
