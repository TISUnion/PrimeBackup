import logging
import threading
from typing import Optional, Callable, List

from mcdreforged.api.all import *

from xbackup import constants
from xbackup.exceptions import BackupNotFound
from xbackup.mcdr.task import TaskEvent, Task
from xbackup.mcdr.task.create_backup_task import CreateBackupTask
from xbackup.mcdr.task.delete_backup_task import DeleteBackupTask
from xbackup.mcdr.task.list_backup_task import ListBackupTask
from xbackup.mcdr.task.restore_backup_task import RestoreBackupTask
from xbackup.mcdr.task_queue import TaskQueue, TaskHolder, TooManyOngoingTask
from xbackup.types.backup_filter import BackupFilter
from xbackup.utils.mcdr_utils import tr


class ThreadedWorker:
	def __init__(self, name: str, logger: logging.Logger, max_ongoing_task: int):
		self.logger = logger
		self.max_ongoing_task = max_ongoing_task
		self.thread = threading.Thread(target=self.__task_loop, name='xbackup-worker-{}@{}'.format(name, constants.INSTANCE_ID))
		self.task_queue: TaskQueue[Optional[TaskHolder]] = TaskQueue(max_ongoing_task)
		self.task_lock = threading.RLock()
		self.current_task_holder: Optional[TaskHolder] = None
		self.current_task_holder_pending_events: List[TaskEvent] = []

	def shutdown(self):
		self.task_queue.put(None)
		self.thread.join()

	def __task_loop(self):
		while True:
			with self.task_lock:
				holder = self.task_queue.get()
			try:
				if holder is None:
					break
				with self.task_lock:
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
				with self.task_lock:
					self.task_queue.task_done()
					self.current_task_holder = None

	def submit(self, source: CommandSource, task_name_key: str, task: Task, callback: Optional[Callable] = None):
		if self.thread.is_alive():
			try:
				self.task_queue.put(TaskHolder(task, tr(task_name_key), source, callback))
			except TooManyOngoingTask:
				if self.max_ongoing_task == 1:
					name = self.current_task_holder.task_name if self.current_task_holder is not None else RText('?')
					source.reply(tr('error.too_much_ongoing_task.exclusive', name))
				else:
					source.reply(tr('error.too_much_ongoing_task.generic', self.max_ongoing_task))
		else:
			source.reply('worker thread is dead, please check logs to see what had happened')

	def send_event_to_current_task(self, event: TaskEvent) -> bool:
		with self.task_lock:
			if self.current_task_holder is not None:
				# task executing, send to it
				self.current_task_holder.task.on_event(event)
			elif self.task_queue.qsize() > 0:
				# there's task in the queue, but the task is not popped yet
				self.current_task_holder_pending_events.append(event)
			else:
				# no task to send event to
				return False


class TaskManager:
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

	# ================================== Interfaces ==================================

	def create_backup(self, source: CommandSource, comment: str):
		self.__add_operate_task(source, 'task.create', CreateBackupTask(source, comment))

	def delete_backup(self, source: CommandSource, backup_id: int):
		self.__add_operate_task(source, 'task.delete', DeleteBackupTask(backup_id))

	def restore_backup(self, source: CommandSource, backup_id: int):
		self.__add_operate_task(source, 'task.restore', RestoreBackupTask(source, backup_id))

	def list_backup(self, source: CommandSource, limit: int, backup_filter: BackupFilter):
		self.__add_read_task(source, 'task.list', ListBackupTask(limit=limit, backup_filter=backup_filter))

	def do_confirm(self) -> bool:
		return self.worker_operator.send_event_to_current_task(TaskEvent.operation_confirmed)

	def do_cancel(self) -> bool:
		return self.worker_operator.send_event_to_current_task(TaskEvent.operation_cancelled)

	def do_abort(self) -> bool:
		return self.worker_operator.send_event_to_current_task(TaskEvent.operation_aborted)

	def on_world_saved(self):
		self.worker_operator.send_event_to_current_task(TaskEvent.world_save_done)

