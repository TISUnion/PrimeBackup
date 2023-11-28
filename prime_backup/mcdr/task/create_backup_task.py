import threading

from mcdreforged.api.all import *

from prime_backup.action.create_backup_action import CreateBackupAction
from prime_backup.mcdr.task import TaskEvent, OperationTask
from prime_backup.types.operator import Operator
from prime_backup.utils.mcdr_utils import Texts
from prime_backup.utils.timer import Timer


class CreateBackupTask(OperationTask):
	def __init__(self, source: CommandSource, comment: str):
		super().__init__(source)
		self.comment = comment
		self.world_saved_done = threading.Event()
		self.plugin_unload = False

	@property
	def name(self) -> str:
		return 'create'

	def run(self):
		self.broadcast(self.tr('start'))

		cmds = self.config.server.commands
		if self.config.server.turn_off_auto_save:
			self.server.execute(cmds.auto_save_off)
		try:
			timer = Timer()
			self.server.execute(cmds.save_all_worlds)
			if len(self.config.server.saved_world_regex) > 0:
				ok = self.world_saved_done.wait(timeout=self.config.server.save_world_max_wait_sec)
				if not ok:
					self.broadcast(self.tr('abort.save_wait_time_out').set_color(RColor.red))
					return
			cost_save_wait = timer.get_and_restart()
			if self.plugin_unload:
				self.broadcast(self.tr('abort.unloaded').set_color(RColor.red))
				return

			action = CreateBackupAction(Operator.of(self.source), self.comment)
			backup = action.run()
			cost_create = timer.get_elapsed()
			cost_total = cost_save_wait + cost_create

			self.logger.info('Time costs: save wait {}s, create backup {}s'.format(round(cost_save_wait, 2), round(cost_create, 2)))
			self.broadcast(self.tr(
				'completed',
				Texts.backup_id(backup.id),
				RText(f'{round(cost_total, 2)}s', RColor.gold),
				Texts.file_size(backup.size),
			))
		except Exception as e:
			self.broadcast(self.tr('failed', e))
			raise
		finally:
			if self.config.server.turn_off_auto_save:
				self.server.execute(cmds.auto_save_on)

	def on_event(self, event: TaskEvent):
		if event == TaskEvent.plugin_unload:
			self.plugin_unload = True
			self.world_saved_done.set()
		elif event == TaskEvent.world_save_done:
			self.world_saved_done.set()
