import threading

from mcdreforged.command.command_source import CommandSource

from prime_backup.action.create_backup_action import CreateBackupAction
from prime_backup.mcdr.task import TaskEvent, Task
from prime_backup.types.operator import Operator


class CreateBackupTask(Task):
	def __init__(self, source: CommandSource, comment: str):
		super().__init__()
		self.action = CreateBackupAction(Operator.of(source), comment)
		self.source = source
		self.server = source.get_server()
		self.world_saved_done = threading.Event()
		self.cancelled = False

	def run(self):
		if self.config.server.turn_off_auto_save:
			self.server.execute(self.config.server.commands.turn_off_auto_save)
		try:
			self.server.execute(self.config.server.commands.save_all_worlds)
			ok = self.world_saved_done.wait(timeout=self.config.server.save_world_max_wait_sec)
			if self.cancelled:
				return   # TODO
			if not ok:
				return  # TODO
			self.action.run()
		finally:
			if self.config.server.turn_off_auto_save:
				self.server.execute(self.config.server.commands.turn_on_auto_save)

	def on_event(self, event: TaskEvent):
		if event == TaskEvent.shutdown:
			self.cancelled = True
			self.world_saved_done.set()
		elif event == TaskEvent.world_save_done:
			self.world_saved_done.set()
