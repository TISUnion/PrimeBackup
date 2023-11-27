import threading

from mcdreforged.command.command_source import CommandSource

from xbackup.task.core.create_backup_task import CreateBackupTask
from xbackup.task.event import TaskEvent
from xbackup.task.types.operator import Operator


class CreateServerBackupTask(CreateBackupTask):
	def __init__(self, source: CommandSource, comment: str):
		super().__init__(Operator.of(source), comment)
		self.source = source
		self.server = source.get_server()
		self.world_saved_done = threading.Event()

	def run(self) -> int:
		if self.config.server.turn_off_auto_save:
			self.server.execute(self.config.server.commands.turn_off_auto_save)
		try:
			self.server.execute(self.config.server.commands.save_all_worlds)
			ok = self.world_saved_done.wait(timeout=self.config.server.save_world_max_wait_sec)
			if not ok:
				pass

			return super().run()
		finally:
			if self.config.server.turn_off_auto_save:
				self.server.execute(self.config.server.commands.turn_on_auto_save)

	def on_event(self, event: TaskEvent):
		if event == TaskEvent.world_save_done:
			self.world_saved_done.set()
