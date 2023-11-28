import collections
import threading
from typing import NamedTuple, Generic, TypeVar, Deque, Union

from mcdreforged.api.all import *

from prime_backup.exceptions import PrimeBackupError
from prime_backup.mcdr.task import Task


class TaskHolder(NamedTuple):
	task: Task
	source: 'CommandSource'

	def task_name(self) -> RTextBase:
		return self.task.get_name_text()


class TooManyOngoingTask(PrimeBackupError):
	def __init__(self, current_item):
		self.current_item = current_item


_T = TypeVar('_T')


class TaskQueue(Generic[_T]):
	class _NoneItem:
		pass

	NONE = _NoneItem()

	def __init__(self, max_ongoing_task: int):
		self.__queue: Deque[_T] = collections.deque()
		self.__unfinished_size = 0
		self.__lock = threading.Lock()
		self.__not_empty = threading.Condition(self.__lock)
		self.__semaphore = threading.Semaphore(max_ongoing_task)
		self.__current_item = self.NONE

	def put(self, task: _T):
		if self.__semaphore.acquire(blocking=False):
			self.put_direct(task)
		else:
			raise TooManyOngoingTask(self.__current_item)

	def put_direct(self, task: _T):
		with self.__lock:
			self.__queue.append(task)
			self.__unfinished_size += 1
			self.__not_empty.notify()

	def get(self) -> _T:
		with self.__not_empty:
			while len(self.__queue) == 0:
				self.__not_empty.wait()
			self.__current_item = item = self.__queue.popleft()
			return item

	def task_done(self):
		with self.__lock:
			self.__semaphore.release()
			self.__unfinished_size -= 1
			self.__current_item = self.NONE

	def qsize(self) -> int:
		with self.__lock:
			return len(self.__queue)

	def unfinished_size(self) -> int:
		with self.__lock:
			return self.__unfinished_size

	@property
	def current_item(self) -> Union[_T, _NoneItem]:
		with self.__lock:
			return self.__current_item
