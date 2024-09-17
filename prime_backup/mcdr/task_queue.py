import collections
import dataclasses
import threading
from concurrent import futures
from typing import Generic, TypeVar, Deque, Union, TYPE_CHECKING, Callable, Optional, Any

from mcdreforged.api.all import *

from prime_backup.exceptions import PrimeBackupError

if TYPE_CHECKING:
	from prime_backup.mcdr.task import Task


_T = TypeVar('_T')
TaskCallback = Callable[[Optional[_T], Optional[Exception]], Any]


@dataclasses.dataclass(frozen=True)
class TaskHolder(Generic[_T]):
	task: 'Task[_T]'
	source: 'CommandSource'
	callback: Optional[TaskCallback[_T]]
	future: 'futures.Future[_T]' = dataclasses.field(default_factory=futures.Future)

	def task_name(self) -> RTextBase:
		return self.task.get_name_text()

	def on_done(self, ret: Optional[_T], err: Optional[Exception]):
		if err is not None:
			self.future.set_exception(err)
		else:
			self.future.set_result(ret)
		if self.callback is not None:
			self.callback(ret, err)


class TaskQueue(Generic[_T]):
	class TooManyOngoingTask(PrimeBackupError):
		def __init__(self, current_item: _T):
			self.current_item: _T = current_item

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
			raise self.TooManyOngoingTask(self.__current_item)

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

	def clear(self):
		with self.__lock:
			n = len(self.__queue)
			self.__queue.clear()
			for _ in range(n):
				self.__semaphore.release()
			self.__unfinished_size -= n

	def qsize(self) -> int:
		with self.__lock:
			return len(self.__queue)

	def peek_first_unfinished_item(self) -> Union[_T, _NoneItem]:
		with self.__lock:
			if self.__current_item is not self.NONE:
				return self.__current_item
			if len(self.__queue) > 0:
				return self.__queue[0]
			else:
				return self.NONE

	def unfinished_size(self) -> int:
		with self.__lock:
			return self.__unfinished_size

	@property
	def current_item(self) -> Union[_T, _NoneItem]:
		with self.__lock:
			return self.__current_item
