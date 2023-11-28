import queue
import threading
from typing import NamedTuple, Generic, TypeVar

from mcdreforged.api.all import *

from prime_backup.exceptions import PrimeBackupError
from prime_backup.mcdr.task import Task


class TaskHolder(NamedTuple):
	task: Task
	source: 'CommandSource'

	@property
	def task_name(self) -> RTextBase:
		return self.task.get_name_text()


class TooManyOngoingTask(PrimeBackupError):
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
