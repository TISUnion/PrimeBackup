import contextlib
import threading
from typing import Optional

from mcdreforged.api.all import CommandSource, RColor
from typing_extensions import override

from prime_backup.action.create_backup_action import CreateBackupAction
from prime_backup.mcdr.task import TaskEvent
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.operator import Operator
from prime_backup.utils.timer import Timer


class CreateBackupTask(HeavyTask[Optional[int]]):
	def __init__(self, source: CommandSource, comment: str, operator: Optional[Operator] = None):
		super().__init__(source)
		self.comment = comment
		if operator is None:
			operator = Operator.of(source)
		self.operator = operator
		self.world_saved_done = threading.Event()
		self.__waiting_world_save = False

	@property
	@override
	def id(self) -> str:
		return 'backup_create'

	@override
	def is_abort_able(self) -> bool:
		return self.__waiting_world_save

	@contextlib.contextmanager
	def __autosave_disabler(self):
		cmd_auto_save_off = self.config.server.commands.auto_save_off
		cmd_auto_save_on = self.config.server.commands.auto_save_on

		applied_auto_save_off = False
		if self.server.is_server_running() and self.config.server.turn_off_auto_save and len(cmd_auto_save_off) > 0:
			self.server.execute(cmd_auto_save_off)
			applied_auto_save_off = True

		try:
			yield
		finally:
			if applied_auto_save_off and self.server.is_server_running() and len(cmd_auto_save_on) > 0:
				self.server.execute(cmd_auto_save_on)

	@override
	def run(self) -> Optional[int]:
		self.broadcast(self.tr('start'))

		with contextlib.ExitStack() as exit_stack:
			exit_stack.enter_context(self.__autosave_disabler())

			timer = Timer()
			if self.server.is_server_running():
				if len(cmd_save_all_worlds := self.config.server.commands.save_all_worlds) > 0:
					self.server.execute(cmd_save_all_worlds)
				if len(self.config.server.saved_world_regex) > 0:
					self.__waiting_world_save = True
					wait_world_saved_done_ok = self.world_saved_done.wait(timeout=self.config.server.save_world_max_wait.value)
					self.__waiting_world_save = False
					if self.aborted_event.is_set():
						self.broadcast(self.get_aborted_text())
						return None
					if not wait_world_saved_done_ok:
						self.broadcast(self.tr('abort.save_wait_time_out').set_color(RColor.red))
						return None
			cost_save_wait = timer.get_and_restart()
			if self.plugin_unloaded_event.is_set():
				self.broadcast(self.tr('abort.unloaded').set_color(RColor.red))
				return None

			action = CreateBackupAction(self.operator, self.comment)
			backup = action.run()
			bls = action.get_new_blobs_summary()
			cost_create = timer.get_elapsed()
			cost_total = cost_save_wait + cost_create

			self.logger.info('Time costs: save wait {}s, create backup {}s'.format(round(cost_save_wait, 2), round(cost_create, 2)))
			self.broadcast(self.tr(
				'completed',
				TextComponents.backup_id(backup.id),
				TextComponents.number(f'{round(cost_total, 2)}s').
				h(self.tr(
					'cost.hover',
					TextComponents.number(f'{round(cost_save_wait, 2)}s'),
					TextComponents.number(f'{round(cost_create, 2)}s')
				)),
				TextComponents.backup_size(backup),
				TextComponents.blob_list_summary_store_size(bls),
			))
			return backup.id

	@override
	def on_event(self, event: TaskEvent):
		super().on_event(event)
		if event == TaskEvent.operation_aborted and self.__waiting_world_save:
			self.world_saved_done.set()
		if event == TaskEvent.plugin_unload:
			self.world_saved_done.set()
		elif event in [TaskEvent.world_save_done, TaskEvent.server_stopped]:
			self.world_saved_done.set()
