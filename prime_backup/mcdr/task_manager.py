import logging
import sqlite3
import threading
from typing import Optional, List

from mcdreforged.api.all import *
from sqlalchemy.exc import OperationalError

from prime_backup import constants
from prime_backup.exceptions import BackupNotFound
from prime_backup.mcdr.task import TaskEvent, Task, OperationTask, ReaderTask, ImmediateTask
from prime_backup.mcdr.task_queue import TaskQueue, TaskHolder
from prime_backup.types.units import Duration
from prime_backup.utils.mcdr_utils import tr, reply_message, mkcmd


class ThreadedWorker:
	def __init__(self, name: str, logger: logging.Logger, max_ongoing_task: int):
		self.name = name
		self.logger = logger
		self.max_ongoing_task = max_ongoing_task
		self.thread = threading.Thread(target=self.__task_loop, name='PB-{}@{}'.format(name, constants.INSTANCE_ID), daemon=True)
		self.stopped = False
		self.task_queue: TaskQueue[Optional[TaskHolder]] = TaskQueue(max_ongoing_task)
		self.current_task_holder_pending_events: List[TaskEvent] = []
		self.current_task_holder_pending_events_lock = threading.Lock()

	def start(self):
		self.thread.start()

	def shutdown(self):
		self.stopped = True
		self.send_event_to_current_task(TaskEvent.plugin_unload)
		self.task_queue.put_direct(None)
		if self.thread.is_alive():
			self.thread.join(Duration('10min').value)

	def __task_loop(self):
		self.logger.info('Worker %s starts', self.name)
		while not self.stopped:
			holder = self.task_queue.get()
			try:
				if holder is None or self.stopped:
					break

				with self.current_task_holder_pending_events_lock:
					events = self.current_task_holder_pending_events.copy()
					self.current_task_holder_pending_events.clear()
				for event in events:
					holder.task.on_event(event)

				holder.task.run()
			except BackupNotFound as e:
				holder.source.reply(tr('error.backup_not_found', e.backup_id).set_color(RColor.red))
			except Exception as e:
				self.logger.exception('Task {} run error'.format(holder.task))
				if isinstance(e, OperationalError) and isinstance(e.orig, sqlite3.OperationalError) and str(e.orig) == 'database is locked':
					holder.source.reply(tr('error.db_locked', holder.task_name()).set_color(RColor.red))
				else:
					holder.source.reply(tr('error.generic', holder.task_name()).set_color(RColor.red))
			finally:
				self.task_queue.task_done()
		self.logger.info('Worker %s stops', self.name)

	def submit(self, source: CommandSource, task: Task):
		if self.thread.is_alive():
			try:
				self.task_queue.put(TaskHolder(task, source))
			except TaskQueue.TooManyOngoingTask as e:
				holder: TaskHolder
				if self.max_ongoing_task == 1 and (holder := e.current_item) is not TaskQueue.NONE:
					name = holder.task_name() if holder is not None else RText('?', RColor.gray)
					reply_message(source, tr('error.too_much_ongoing_task.exclusive', name))
					if holder.task.is_abort_able():
						cmd = mkcmd('abort')
						reply_message(source, tr('error.too_much_ongoing_task.try_abort').h(cmd).c(RAction.suggest_command, cmd))
				else:
					reply_message(source, tr('error.too_much_ongoing_task.generic', self.max_ongoing_task))
		else:
			source.reply('worker thread is dead, please check logs to see what had happened')

	def send_event_to_current_task(self, event: TaskEvent) -> bool:
		current_task = self.task_queue.current_item
		if current_task not in (None, TaskQueue.NONE):
			# a task is executing, send to it
			current_task.task.on_event(event)
		elif self.task_queue.unfinished_size() > 0:
			with self.current_task_holder_pending_events_lock:
				# there's task in the queue, but the task is not popped yet
				self.current_task_holder_pending_events.append(event)
		else:
			# no task to send event to
			return False
		return True


class TaskManager:
	def __init__(self, server: PluginServerInterface):
		self.server = server
		self.logger = server.logger
		self.worker_operator = ThreadedWorker('operator', self.logger, 1)
		self.worker_reader = ThreadedWorker('reader', self.logger, 3)

	def start(self):
		self.worker_operator.start()
		self.worker_reader.start()

	def shutdown(self):
		self.worker_operator.shutdown()
		self.worker_reader.shutdown()

	# ================================== Interfaces ==================================

	def add_task(self, task: Task):
		source = task.source
		if isinstance(task, OperationTask):
			self.worker_operator.submit(source, task)
		elif isinstance(task, ReaderTask):
			self.worker_reader.submit(source, task)
		elif isinstance(task, ImmediateTask):
			task.run()
		else:
			raise TypeError(type(task))

	def do_confirm(self) -> bool:
		return self.worker_operator.send_event_to_current_task(TaskEvent.operation_confirmed)

	def do_abort(self) -> bool:
		return self.worker_operator.send_event_to_current_task(TaskEvent.operation_aborted)

	def on_world_saved(self):
		self.worker_operator.send_event_to_current_task(TaskEvent.world_save_done)

