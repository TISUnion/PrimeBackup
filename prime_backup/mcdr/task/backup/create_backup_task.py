import threading
from typing import Optional

from mcdreforged.api.all import *

from prime_backup.action.create_backup_action import CreateBackupAction
from prime_backup.mcdr.task import TaskEvent
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.operator import Operator
from prime_backup.utils.timer import Timer


class CreateBackupTask(HeavyTask[None]):
	def __init__(self, source: CommandSource, comment: str, operator: Optional[Operator] = None):
		super().__init__(source)
		self.comment = comment
		if operator is None:
			operator = Operator.of(source)
		self.operator = operator
		self.world_saved_done = threading.Event()

	@property
	def id(self) -> str:
		return 'backup_create'

	def run(self):
		self.broadcast(self.tr('start'))

		cmds = self.config.server.commands
		applied_auto_save_off = False
		if self.server.is_server_running() and self.config.server.turn_off_auto_save and len(cmds.auto_save_off) > 0:
			self.server.execute(cmds.auto_save_off)
			applied_auto_save_off = True
		try:
			timer = Timer()
			if self.server.is_server_running():
				if len(cmds.save_all_worlds) > 0:
					self.server.execute(cmds.save_all_worlds)
				if len(self.config.server.saved_world_regex) > 0:
					ok = self.world_saved_done.wait(timeout=self.config.server.save_world_max_wait.value)
					if not ok:
						self.broadcast(self.tr('abort.save_wait_time_out').set_color(RColor.red))
						return
			cost_save_wait = timer.get_and_restart()
			if self.plugin_unloaded_event.is_set():
				self.broadcast(self.tr('abort.unloaded').set_color(RColor.red))
				return

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
		finally:
			if applied_auto_save_off and len(cmds.auto_save_on) > 0:
				self.server.execute(cmds.auto_save_on)

	def on_event(self, event: TaskEvent):
		super().on_event(event)
		if event == TaskEvent.plugin_unload:
			self.world_saved_done.set()
		elif event in [TaskEvent.world_save_done, TaskEvent.server_stopped]:
			self.world_saved_done.set()
