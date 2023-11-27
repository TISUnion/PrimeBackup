import queue
import threading
from typing import Optional, NamedTuple, Callable, Generic, TypeVar

from mcdreforged.api.all import *

from xbackup.exceptions import XBackupError
from xbackup.mcdr.task import Task


class TaskHolder(NamedTuple):
	task: Task
	task_name: 'RTextBase'
	source: 'CommandSource'
	callback: Optional[Callable]


class TooManyOngoingTask(XBackupError):
	pass


_T = TypeVar('_T')


class TaskQueue(Generic[_T]):
	def __init__(self, max_ongoing_task: int):
		self.max_ongoing_task = max_ongoing_task
		self.queue = queue.Queue()
		self.semaphore = threading.Semaphore(max_ongoing_task)

	def put(self, task: _T):
		if self.semaphore.acquire(blocking=False):
			self.queue.put(task)
		else:
			raise TooManyOngoingTask()

	def get(self) -> _T:
		return self.queue.get()

	def task_done(self):
		self.queue.task_done()
		self.semaphore.release()

	def qsize(self) -> int:
		return self.queue.qsize()
