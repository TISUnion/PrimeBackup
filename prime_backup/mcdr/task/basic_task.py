import threading
from abc import ABC
from typing import Union, Optional, TypeVar

from mcdreforged.api.all import *
from typing_extensions import final

from prime_backup.action import Action
from prime_backup.mcdr.task import Task, TaskEvent
from prime_backup.mcdr.task.task_utils import ConfirmHelper
from prime_backup.types.units import Duration
from prime_backup.utils import mcdr_utils
from prime_backup.utils.mcdr_utils import TranslationContext

_S = TypeVar('_S')
_T = TypeVar('_T')


class _BasicTask(Task[_T], ABC):
	__base_tr = TranslationContext('task._base').tr

	def __init__(self, source: CommandSource):
		super().__init__(source)

		from prime_backup import logger
		from prime_backup.config.config import Config
		self.logger = logger.get()
		self.config = Config.get()

		self.aborted_event = threading.Event()
		self.plugin_unloaded_event = threading.Event()
		self.is_waiting_confirm = False
		self._confirm_helper = ConfirmHelper(self.source)
		self._quiet = False

		self.__running_action: Optional[Action] = None
		self.__running_subtask: Optional[Task] = None

	# ================================== Overrides ==================================

	def is_abort_able(self) -> bool:
		return (
				self.is_waiting_confirm
				or ((task := self.__running_subtask) is not None and task.is_abort_able())
				or ((action := self.__running_action) is not None and action.is_interruptable())
		)

	def get_abort_permission(self) -> int:
		return max(PermissionLevel.ADMIN, self.source.get_permission_level())

	def on_event(self, event: TaskEvent):
		self._confirm_helper.on_event(event)
		if event in [TaskEvent.plugin_unload, TaskEvent.operation_aborted]:
			if event == TaskEvent.plugin_unload:
				self.plugin_unloaded_event.set()
			self.aborted_event.set()
		if (action := self.__running_action) is not None:
			action.interrupt()
		if (task := self.__running_subtask) is not None:
			task.on_event(event)

	# ==================================== Utils ====================================

	def get_aborted_text(self) -> RTextBase:
		return self.__base_tr('aborted', self.get_name_text())

	def wait_confirm(self, confirm_target_text: Optional[RTextBase] = None, time_wait: Optional[Duration] = None) -> bool:
		if time_wait is None:
			time_wait = self.config.command.confirm_time_wait

		self.is_waiting_confirm = True
		try:
			wr = self._confirm_helper.wait_confirm(confirm_target_text, time_wait)
			if not wr.is_set():
				self.broadcast(self.__base_tr('no_confirm', self.get_name_text()))
				return False
			elif wr.get().is_cancelled():
				self.broadcast(self.get_aborted_text())
				return False
			else:
				return True
		finally:
			self.is_waiting_confirm = False

	def run_action(self, action: Action[_S], auto_interrupt: bool = True) -> _S:
		self.__running_action = action
		if auto_interrupt and self.aborted_event.is_set():
			action.interrupt()
		try:
			return action.run()
		finally:
			self.__running_action = None

	def run_subtask(self, task: Task[_S]) -> _S:
		self.__running_subtask = task
		try:
			return task.run()
		finally:
			self.__running_subtask = None

	def reply(self, msg: Union[str, RTextBase], *, with_prefix: bool = True):
		if self._quiet:
			return
		mcdr_utils.reply_message(self.source, msg, with_prefix=with_prefix)

	def reply_tr(self, key: str, *args, **kwargs):
		with_prefix = kwargs.pop('with_prefix', True)
		self.reply(self.tr(key, *args, **kwargs), with_prefix=with_prefix)

	def broadcast(self, msg: Union[str, RTextBase], *, with_prefix: bool = True):
		if self._quiet:
			return
		mcdr_utils.broadcast_message(msg, with_prefix=with_prefix)


class HeavyTask(_BasicTask[_T], ABC):
	"""
	For tasks that require DB access and does some operations on blobs / database
	"""
	MAX_ONGOING_TASK = 1


class LightTask(_BasicTask[_T], ABC):
	"""
	For tasks that require DB access and runs fast
	"""
	MAX_ONGOING_TASK = 5


class ImmediateTask(_BasicTask[_T], ABC):
	"""
	For tasks that do not require DB access

	Executes immediately
	"""
	@final
	def is_abort_able(self) -> bool:
		return super().is_abort_able()
