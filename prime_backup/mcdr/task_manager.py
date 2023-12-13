import contextlib
import enum
import sqlite3
import threading
from typing import Optional, Callable

from mcdreforged.api.all import *
from sqlalchemy.exc import OperationalError

from prime_backup import logger
from prime_backup.exceptions import BackupNotFound
from prime_backup.mcdr.task import TaskEvent, Task
from prime_backup.mcdr.task.basic_task import HeavyTask, LightTask, ImmediateTask
from prime_backup.mcdr.task_queue import TaskQueue, TaskHolder, TaskCallback
from prime_backup.types.units import Duration
from prime_backup.utils import misc_utils, mcdr_utils
from prime_backup.utils.mcdr_utils import tr, reply_message, mkcmd


class SendEventResult(enum.Enum):
	success = enum.auto()
	no_task = enum.auto()
	not_your_task = enum.auto()
	no_permission = enum.auto()


class _TaskWorker:
	def __init__(self, name: str, max_ongoing_task: int):
		self.name = name
		self.logger = logger.get()
		self.max_ongoing_task = max_ongoing_task
		self.thread = threading.Thread(target=self.__task_loop, name=misc_utils.make_thread_name(f'worker-{name}'), daemon=True)
		self.stopped = False
		self.task_queue: TaskQueue[Optional[TaskHolder]] = TaskQueue(max_ongoing_task)

	def start(self):
		self.thread.start()

	def shutdown(self):
		self.stopped = True
		self.send_event_to_current_task(TaskEvent.plugin_unload)
		self.task_queue.put_direct(None)
		if self.thread.is_alive():
			self.thread.join(Duration('10min').value)

	@classmethod
	def run_task(cls, holder: TaskHolder) -> Optional[Exception]:
		try:
			ret = holder.task.run()
		except Exception as e:
			holder.run_callback(None, e)

			if isinstance(e, BackupNotFound):
				reply_message(holder.source, tr('error.backup_not_found', e.backup_id).set_color(RColor.red))
				return

			logger.get().exception('Task {} run error'.format(holder.task))
			if isinstance(e, OperationalError) and isinstance(e.orig, sqlite3.OperationalError) and str(e.orig) == 'database is locked':
				reply_message(holder.source, tr('error.db_locked', holder.task_name()).set_color(RColor.red))
			else:
				reply_message(holder.source, tr('error.generic', holder.task_name()).set_color(RColor.red))
		else:
			holder.run_callback(ret, None)

	def __task_loop(self):
		self.logger.info('Worker %s started', self.name)
		while not self.stopped:
			holder = self.task_queue.get()
			with contextlib.ExitStack() as exit_stack:
				exit_stack.callback(self.task_queue.task_done)

				if holder is None or self.stopped:
					break

				self.run_task(holder)

		self.logger.info('Worker %s stopped', self.name)

	def submit(self, task_holder: TaskHolder, *, handle_tmo_err: bool = True):
		source, callback = task_holder.source, task_holder.callback
		if self.thread.is_alive():
			try:
				self.task_queue.put(task_holder)
			except TaskQueue.TooManyOngoingTask as e:
				if not handle_tmo_err:
					raise

				holder: TaskHolder
				if self.max_ongoing_task == 1 and (holder := e.current_item) is not TaskQueue.NONE:
					name = holder.task_name() if holder is not None else RText('?', RColor.gray)
					reply_message(source, tr('error.too_much_ongoing_task.exclusive', name))
					if holder.task.is_abort_able():
						cmd = mkcmd('abort')
						reply_message(
							source,
							tr('error.too_much_ongoing_task.try_abort').
							h(tr('error.too_much_ongoing_task.try_abort.hover', RText(cmd, RColor.gray))).
							c(RAction.suggest_command, cmd)
						)
				else:
					reply_message(source, tr('error.too_much_ongoing_task.generic', self.max_ongoing_task))
		else:
			source.reply('worker thread is dead, please check logs to see what had happened')
			if callback is not None:
				callback(None, RuntimeError('worker dead'))

	def send_event_to_current_task(
			self, event: TaskEvent, *,
			task_checker: Optional[Callable[[TaskHolder], Optional[SendEventResult]]] = None
	) -> SendEventResult:
		task_holder = self.task_queue.peek_first_unfinished_item()
		if task_holder not in (None, TaskQueue.NONE):
			if task_checker is not None and (cr := task_checker(task_holder)) is not None:
				return cr
			task_holder.task.on_event(event)
			return SendEventResult.success
		else:
			return SendEventResult.no_task


class TaskManager:
	def __init__(self):
		self.logger = logger.get()
		self.worker_heavy = _TaskWorker('heavy', HeavyTask.MAX_ONGOING_TASK)
		self.worker_light = _TaskWorker('light', LightTask.MAX_ONGOING_TASK)

	def start(self):
		self.worker_heavy.start()
		self.worker_light.start()

	def shutdown(self):
		self.worker_heavy.shutdown()
		self.worker_light.shutdown()

	# ================================== Interfaces ==================================

	def add_task(self, task: Task, callback: Optional[TaskCallback] = None, *, handle_tmo_err: bool = True):
		source = task.source
		holder = TaskHolder(task, source, callback)
		if isinstance(task, HeavyTask):
			self.worker_heavy.submit(holder, handle_tmo_err=handle_tmo_err)
		elif isinstance(task, LightTask):
			self.worker_light.submit(holder, handle_tmo_err=handle_tmo_err)
		elif isinstance(task, ImmediateTask):
			_TaskWorker.run_task(holder)
		else:
			raise TypeError(type(task))

	def do_confirm(self, source: CommandSource) -> SendEventResult:
		def check_confirm_able(holder: TaskHolder) -> Optional[SendEventResult]:
			if mcdr_utils.are_source_equals(source, holder.source):
				return  # you can confirm you task

			if source.get_permission_level() >= max(PermissionLevel.ADMIN, holder.source.get_permission_level()):
				return  # admin and above can confirm other people's tasks, if they have enough permission

			return SendEventResult.not_your_task

		return self.worker_heavy.send_event_to_current_task(TaskEvent.operation_confirmed, task_checker=check_confirm_able)

	def do_abort(self, source: CommandSource) -> SendEventResult:
		def check_abort_able(holder: TaskHolder) -> Optional[SendEventResult]:
			if mcdr_utils.are_source_equals(source, holder.source):
				return  # you can abort your task

			if source.get_permission_level() >= holder.task.get_abort_permission():
				return  # or your permission needs to >= the task's requirement

			return SendEventResult.no_permission

		return self.worker_heavy.send_event_to_current_task(TaskEvent.operation_aborted, task_checker=check_abort_able)

	def on_world_saved(self):
		self.worker_heavy.send_event_to_current_task(TaskEvent.world_save_done)

