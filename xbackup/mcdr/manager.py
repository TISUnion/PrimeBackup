import logging
import threading
from typing import Optional, Callable, List

from mcdreforged.api.all import *

from xbackup import constants
from xbackup.exceptions import BackupNotFound
from xbackup.mcdr.task_queue import TaskQueue, TaskHolder, TooManyOngoingTask
from xbackup.task.cmd.create_server_backup_task import CreateServerBackupTask
from xbackup.task.cmd.list_backup_task import ListBackupTask
from xbackup.task.cmd.restore_backup_task import RestoreServerBackupTask
from xbackup.task.core.delete_backup_task import DeleteBackupTask
from xbackup.task.event import TaskEvent
from xbackup.task.task import Task
from xbackup.task.types.backup_info import BackupInfo
from xbackup.utils.mcdr_utils import tr


class ThreadedWorker:
	def __init__(self, name: str, logger: logging.Logger, max_ongoing_task: int):
		self.logger = logger
		self.max_ongoing_task = max_ongoing_task
		self.thread = threading.Thread(target=self.__task_loop, name='xbackup-worker-{}@{}'.format(name, constants.INSTANCE_ID))
		self.task_queue: TaskQueue[Optional[TaskHolder]] = TaskQueue(max_ongoing_task)
		self.current_task_holder_lock = threading.RLock()
		self.current_task_holder: Optional[TaskHolder] = None
		self.current_task_holder_pending_events: List[TaskEvent] = []

	def shutdown(self):
		self.task_queue.put(None)
		self.thread.join()

	def __task_loop(self):
		while True:
			holder = self.task_queue.get()
			try:
				if holder is None:
					break

				with self.current_task_holder_lock:
					self.current_task_holder = holder
					for event in self.current_task_holder_pending_events:
						holder.task.on_event(event)
					self.current_task_holder_pending_events.clear()

				ret = holder.task.run()
				if holder.callback is not None:
					holder.callback(ret)

			except BackupNotFound as event:
				self.logger.warning('backup %s not found', event.backup_id)
				holder.source.reply(tr('error.backup_not_found', event.backup_id))
			except Exception:
				self.logger.exception('Task {} run error'.format(holder.task))
				holder.source.reply(tr('error.generic', holder.task_name))
			finally:
				self.task_queue.task_done()
				with self.current_task_holder_lock:
					self.current_task_holder = None

	def submit(self, source: CommandSource, task_name_key: str, task: Task, callback: Optional[Callable] = None):
		if self.thread.is_alive():
			try:
				self.task_queue.put(TaskHolder(task, tr(task_name_key), source, callback))
			except TooManyOngoingTask:
				if self.max_ongoing_task == 1:
					name = self.current_task_holder.task_name if self.current_task_holder is not None else RText('?')
					source.reply(tr('too_much_ongoing_task.exclusive', name))
				else:
					source.reply(tr('too_much_ongoing_task.generic', self.max_ongoing_task))
		else:
			source.reply('worker thread is dead, please check logs to see what had happened')

	def send_event_to_current_task(self, event: TaskEvent):
		with self.current_task_holder_lock:
			if self.current_task_holder is not None:
				self.current_task_holder.task.on_event(event)
			else:
				self.current_task_holder_pending_events.append(event)


class Manager:
	def __init__(self, server: PluginServerInterface):
		self.server = server
		self.logger = server.logger
		self.worker_operator = ThreadedWorker('operator', self.logger, 1)
		self.worker_reader = ThreadedWorker('reader', self.logger, 10)

	def shutdown(self):
		self.worker_operator.shutdown()
		self.worker_reader.shutdown()

	def __add_operate_task(self, source: CommandSource, task_name_key: str, task: Task, callback: Optional[Callable] = None):
		self.worker_operator.submit(source, task_name_key, task, callback)

	def __add_read_task(self, source: CommandSource, task_name_key: str, task: Task, callback: Optional[Callable] = None):
		self.worker_reader.submit(source, task_name_key, task, callback)

	def create_backup(self, source: CommandSource, comment: str):
		self.__add_operate_task(source, 'task.create', CreateServerBackupTask(source, comment))

	def delete_backup(self, source: CommandSource, backup_id: int):
		self.__add_operate_task(source, 'task.delete', DeleteBackupTask(backup_id))

	def restore_backup(self, source: CommandSource, backup_id: int):
		self.__add_operate_task(source, 'task.restore', RestoreServerBackupTask(source, backup_id))

	def list_backup(self, source: CommandSource):
		def callback(backups: List[BackupInfo]):
			for backup in backups:
				# TODO better display
				source.reply(str(backup))

		self.__add_read_task(source, 'task.list', ListBackupTask(), callback)

	def on_world_saved(self):
		self.worker_operator.send_event_to_current_task(TaskEvent.world_save_done)

