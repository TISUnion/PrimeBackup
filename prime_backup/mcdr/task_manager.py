import logging
import threading
from typing import Optional, List

from mcdreforged.api.all import *

from prime_backup import constants
from prime_backup.exceptions import BackupNotFound
from prime_backup.mcdr.task import TaskEvent, Task, TaskType
from prime_backup.mcdr.task_queue import TaskQueue, TaskHolder, TooManyOngoingTask
from prime_backup.utils.mcdr_utils import tr


class ThreadedWorker:
	def __init__(self, name: str, logger: logging.Logger, max_ongoing_task: int):
		self.logger = logger
		self.max_ongoing_task = max_ongoing_task
		self.thread = threading.Thread(target=self.__task_loop, name='PB-worker-{}@{}'.format(name, constants.INSTANCE_ID))
		self.stopped = False
		self.task_queue: TaskQueue[Optional[TaskHolder]] = TaskQueue(max_ongoing_task)
		self.task_lock = threading.RLock()
		self.current_task_holder: Optional[TaskHolder] = None
		self.current_task_holder_pending_events: List[TaskEvent] = []

	def shutdown(self):
		self.stopped = True
		self.send_event_to_current_task(TaskEvent.plugin_unload)
		self.task_queue.put(None)
		self.thread.join()

	def __task_loop(self):
		while not self.stopped:
			with self.task_lock:
				holder = self.task_queue.get()
			try:
				if holder is None or self.stopped:
					break
				with self.task_lock:
					self.current_task_holder = holder
					for event in self.current_task_holder_pending_events:
						holder.task.on_event(event)
					self.current_task_holder_pending_events.clear()

				holder.task.run()
			except BackupNotFound as e:
				holder.source.reply(tr('error.backup_not_found', e.backup_id).set_color(RColor.red))
			except Exception:
				self.logger.exception('Task {} run error'.format(holder.task))
				holder.source.reply(tr('error.generic', holder.task_name).set_color(RColor.red))
			finally:
				with self.task_lock:
					self.task_queue.task_done()
					self.current_task_holder = None

	def submit(self, source: CommandSource, task: Task):
		if self.thread.is_alive():
			try:
				self.task_queue.put(TaskHolder(task, source))
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

	# ================================== Interfaces ==================================

	def add_task(self, task: Task):
		source = task.source
		if task.type == TaskType.operate:
			self.worker_operator.submit(source, task)
		elif task.type == TaskType.read:
			self.worker_reader.submit(source, task)
		elif task.type == TaskType.immediate:
			task.run()
		else:
			raise TypeError(task.type)

	def do_confirm(self) -> bool:
		return self.worker_operator.send_event_to_current_task(TaskEvent.operation_confirmed)

	def do_cancel(self) -> bool:
		return self.worker_operator.send_event_to_current_task(TaskEvent.operation_cancelled)

	def do_abort(self) -> bool:
		return self.worker_operator.send_event_to_current_task(TaskEvent.operation_aborted)

	def on_world_saved(self):
		self.worker_operator.send_event_to_current_task(TaskEvent.world_save_done)

