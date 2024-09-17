import contextlib
import dataclasses
import enum
import sqlite3
import threading
from concurrent import futures
from typing import Optional, Callable, Any, TypeVar

from mcdreforged.api.all import *
from sqlalchemy.exc import OperationalError

from prime_backup import logger
from prime_backup.exceptions import BackupNotFound, BackupFileNotFound, BlobNotFound, BlobHashNotUnique
from prime_backup.mcdr.task import TaskEvent, Task
from prime_backup.mcdr.task.basic_task import HeavyTask, LightTask, ImmediateTask
from prime_backup.mcdr.task_queue import TaskQueue, TaskHolder, TaskCallback
from prime_backup.types.units import Duration
from prime_backup.utils import misc_utils, mcdr_utils
from prime_backup.utils.mcdr_utils import tr, reply_message, mkcmd

_T = TypeVar('_T')


class _SendEventStatus(enum.Enum):
	sent = enum.auto()
	missed = enum.auto()
	failed = enum.auto()


@dataclasses.dataclass(frozen=True)
class _SendEventResult:
	status: _SendEventStatus
	holder: Optional[TaskHolder]


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
		self.task_queue.clear()
		self.task_queue.put_direct(None)
		if self.thread.is_alive():
			self.thread.join(Duration('1h').value)

	@classmethod
	def __handle_common_exceptions(cls, holder: TaskHolder, e: Exception) -> bool:
		if isinstance(e, BackupNotFound):
			lines = [tr('error.backup_not_found', e.backup_id)]
		elif isinstance(e, BackupFileNotFound):
			lines = [tr('error.backup_file_not_found', e.backup_id, e.path)]
		elif isinstance(e, BlobNotFound):
			lines = [tr('error.blob_not_found', e.blob_hash)]
		elif isinstance(e, BlobHashNotUnique):
			lines = [
				tr('error.blob_hash_not_unique', e.blob_hash_prefix),
				tr('error.blob_hash_not_unique.candidates', len(e.candidates), ', '.join([b.hash for b in e.candidates])),
			]
		else:
			return False

		for text in lines:
			reply_message(holder.source, text.set_color(RColor.red))
		return True

	@classmethod
	def run_task(cls, holder: TaskHolder) -> Optional[Exception]:
		try:
			ret = holder.task.run()
		except Exception as e:
			holder.on_done(None, e)

			if cls.__handle_common_exceptions(holder, e):
				return

			logger.get().exception('Task {} run error'.format(holder.task))
			if isinstance(e, OperationalError) and isinstance(e.orig, sqlite3.OperationalError) and str(e.orig) == 'database is locked':
				reply_message(holder.source, tr('error.db_locked', holder.task_name()).set_color(RColor.red))
			else:
				reply_message(holder.source, tr('error.generic', holder.task_name()).set_color(RColor.red))
		else:
			holder.on_done(ret, None)

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
			task_checker: Optional[Callable[[TaskHolder], bool]] = None,
			pre_send_callback: Optional[Callable[[TaskHolder], Any]] = None,
	) -> _SendEventResult:
		task_holder = self.task_queue.peek_first_unfinished_item()
		if task_holder not in (None, TaskQueue.NONE):
			if task_checker is not None and not task_checker(task_holder):
				return _SendEventResult(_SendEventStatus.failed, None)

			if pre_send_callback is not None:
				pre_send_callback(task_holder)
			task_holder.task.on_event(event)
			return _SendEventResult(_SendEventStatus.sent, task_holder)
		else:
			return _SendEventResult(_SendEventStatus.missed, None)


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

	def add_task(self, task: Task[_T], callback: Optional[TaskCallback[_T]] = None, *, handle_tmo_err: bool = True) -> 'futures.Future[_T]':
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
		return holder.future

	def do_confirm(self, source: CommandSource):
		def check_confirm_able(holder: TaskHolder) -> bool:
			if mcdr_utils.are_source_same(source, holder.source):
				return True  # you can confirm you task

			if source.get_permission_level() >= max(PermissionLevel.ADMIN, holder.source.get_permission_level()):
				return True  # admin and above can confirm other people's tasks, if they have enough permission

			reply_message(source, tr('command.confirm.not_your_task'))
			return False

		def pre_send(holder: TaskHolder):
			reply_message(source, tr('command.confirm.sent', holder.task_name()))

		result = self.worker_heavy.send_event_to_current_task(TaskEvent.operation_confirmed, task_checker=check_confirm_able, pre_send_callback=pre_send)
		if result.status == _SendEventStatus.missed:
			reply_message(source, tr('command.confirm.noop'))

	def do_abort(self, source: CommandSource):
		def check_abort_able(holder: TaskHolder) -> bool:
			if not (
				# you can abort your task
				mcdr_utils.are_source_same(source, holder.source)
				# or your permission needs to >= the task's requirement
				or source.get_permission_level() >= holder.task.get_abort_permission()
			):
				reply_message(source, tr('command.abort.no_permission'))
				return False

			if not holder.task.is_abort_able():
				reply_message(source, tr('command.abort.not_abort_able', holder.task_name()))
				return False

			return True

		def pre_send(holder: TaskHolder):
			reply_message(source, tr('command.abort.sent', holder.task_name()))

		result = self.worker_heavy.send_event_to_current_task(TaskEvent.operation_aborted, task_checker=check_abort_able, pre_send_callback=pre_send)
		if result.status == _SendEventStatus.missed:
			reply_message(source, tr('command.abort.noop'))

	def on_world_saved(self):
		self.worker_heavy.send_event_to_current_task(TaskEvent.world_save_done)

	def on_server_stopped(self):
		self.worker_heavy.send_event_to_current_task(TaskEvent.server_stopped)

