import enum

from mcdreforged.api.all import RTextBase, CommandSource

from prime_backup.mcdr.task import TaskEvent
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.units import Duration
from prime_backup.utils.mcdr_utils import reply_message
from prime_backup.utils.waitable_value import WaitableValue


class ConfirmResult(enum.Enum):
	confirmed = enum.auto()
	cancelled = enum.auto()

	def is_confirmed(self):
		return self == ConfirmResult.confirmed

	def is_cancelled(self):
		return self == ConfirmResult.cancelled


class ConfirmHelper:
	def __init__(self, source: CommandSource):
		self.source = source
		self.__confirm_result: WaitableValue[ConfirmResult] = WaitableValue()

	def wait_confirm(self, confirm_target_text: RTextBase, time_wait: Duration) -> WaitableValue[ConfirmResult]:
		text = TextComponents.confirm_hint(confirm_target_text, TextComponents.duration(time_wait))
		reply_message(self.source, text)
		self.__confirm_result.clear()
		self.__confirm_result.wait(time_wait.value)
		return self.__confirm_result

	def on_event(self, event: TaskEvent):
		if event in [TaskEvent.plugin_unload, TaskEvent.operation_aborted]:
			self.__confirm_result.set(ConfirmResult.cancelled)
		elif event == TaskEvent.operation_confirmed:
			self.__confirm_result.set(ConfirmResult.confirmed)
