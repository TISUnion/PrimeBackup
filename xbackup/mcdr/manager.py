import queue
import threading
from typing import Optional

from mcdreforged.api.all import CommandSource, PluginServerInterface

from xbackup import constants
from xbackup.config.sub_configs import BackupJob
from xbackup.task.create_backup_task import CreateBackupTask
from xbackup.task.task import Task


class Manager:
	def __init__(self, server: PluginServerInterface):
		self.server = server
		self.logger = server.logger
		self.thread = threading.Thread(target=self.__task_loop, name='XBackupMain@{}'.format(constants.INSTANCE_ID))
		self.task_queue: queue.Queue[Optional[Task]] = queue.Queue(maxsize=1)

	def shutdown(self):
		self.task_queue.put(None)
		self.thread.join()

	def __task_loop(self):
		while True:
			task = self.task_queue.get()
			if task is None:
				break
			try:
				task.run()
			except Exception:
				self.logger.exception('Session {} run error'.format(task))

	def create_backup(self, source: CommandSource, job: BackupJob, comment: str):
		# TODO: spam proof
		if source.is_player:
			author = f'player:{source.player}'
		elif source.is_console:
			author = f'console'
		else:
			author = f'other:{source}'
		self.task_queue.put(CreateBackupTask(job, author, comment))

	def on_world_saved(self):
		pass

