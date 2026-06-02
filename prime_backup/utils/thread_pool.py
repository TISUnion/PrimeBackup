import dataclasses
import multiprocessing
import threading
from concurrent.futures import ThreadPoolExecutor, Future, ProcessPoolExecutor
from typing import List, TypeVar, Callable, Optional, Union, TYPE_CHECKING, Any

from typing_extensions import override, ParamSpec

from prime_backup.utils import misc_utils
from prime_backup.utils.run_once import RunOnceFunc

if TYPE_CHECKING:
	from prime_backup.types.hash_method import HashMethod
	from multiprocessing.synchronize import Semaphore as MpSemaphore

_T = TypeVar('_T')
_P = ParamSpec('_P')


@dataclasses.dataclass(frozen=True)
class _BasePool:
	submit: Callable[[Any, Any, Any], Future[Any]]
	exit: Callable[[Any, Any, Any], Optional[bool]]


class _FailFastConcurrentPoolHelper:
	if TYPE_CHECKING:
		_Pool = Union[ThreadPoolExecutor, ProcessPoolExecutor]
		_Semaphore = Union[threading.Semaphore, MpSemaphore]

	def __init__(self, pool: _BasePool, sem: '_Semaphore'):
		self.__pool = pool
		self.__sem = sem

		self.__all_futures: List[Future] = []
		self.__error_future: Optional[Future] = None
		self.__error_future_lock = threading.Lock()

	def __raise_error_if_any(self):
		# re-raise the worker exception
		with self.__error_future_lock:
			if self.__error_future is not None:
				self.__error_future.result()

	def submit(self, fn: Callable[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs) -> 'Future[_T]':
		self.__raise_error_if_any()
		self.__sem.acquire()
		sem_releaser = RunOnceFunc(self.__sem.release)
		try:
			self.__raise_error_if_any()

			def wrapper_done_callback(done_future: Future):
				try:
					done_future.result()
				except BaseException:
					with self.__error_future_lock:
						if self.__error_future is None:
							self.__error_future = done_future
				finally:
					sem_releaser()

			future: 'Future[_T]' = self.__pool.submit(fn, *args, **kwargs)
			future.add_done_callback(wrapper_done_callback)
			self.__all_futures.append(future)
			return future
		except Exception:
			sem_releaser()
			raise

	def __exit__(self, exc_type, exc_val, exc_tb):
		if exc_type is None:
			self.wait_and_ensure_no_error()
		return self.__pool.exit(exc_type, exc_val, exc_tb)

	def wait_and_ensure_no_error(self):
		for future in self.__all_futures:
			future.result()


def _compute_max_workers(max_workers: Optional[int]) -> int:
	if max_workers is None:
		from prime_backup.config.config import Config
		max_workers = Config.get().get_effective_concurrency()
	return max_workers


class FailFastBlockingThreadPool(ThreadPoolExecutor):
	"""
	A thread pool that:
	1. submit() calls will be blocked if there's no free worker
	2. makes exception raise as soon as possible
	3. no more task will be submitted after an exception raises
	4. nested submit() from worker threads is unsupported and may deadlock
	"""

	def __init__(self, name: str, max_workers: Optional[int] = None):
		max_workers = _compute_max_workers(max_workers)
		thread_name_prefix = misc_utils.make_thread_name(name)
		super().__init__(max_workers=max_workers, thread_name_prefix=thread_name_prefix)
		self.__helper = _FailFastConcurrentPoolHelper(_BasePool(super().submit, super().__exit__), threading.Semaphore(max_workers))

	@override
	def submit(self, fn: Callable[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs) -> 'Future[_T]':
		return self.__helper.submit(fn, *args, **kwargs)

	@override
	def __exit__(self, exc_type, exc_val, exc_tb):
		return self.__helper.__exit__(exc_type, exc_val, exc_tb)

	def wait_and_ensure_no_error(self):
		self.__helper.wait_and_ensure_no_error()


class FailFastBlockingProcessPool(ProcessPoolExecutor):
	"""
	A process pool that:
	1. submit() calls will be blocked if there's no free worker
	2. makes exception raise as soon as possible
	3. no more task will be submitted after an exception raises
	4. nested submit() from worker threads is unsupported and may deadlock
	"""

	def __init__(self, max_workers: Optional[int] = None):
		from prime_backup.db.access import DbAccess
		max_workers = _compute_max_workers(max_workers)
		super().__init__(max_workers=max_workers, initializer=self._child_initializer, initargs=(DbAccess.get_hash_method(),))
		self.__helper = _FailFastConcurrentPoolHelper(_BasePool(super().submit, super().__exit__), multiprocessing.Semaphore(max_workers))

	@classmethod
	def _child_initializer(cls, hash_method: 'HashMethod'):
		from prime_backup.db.access import DbAccess
		# noinspection PyProtectedMember
		DbAccess._set_hash_method(hash_method)

	@override
	def submit(self, fn: Callable[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs) -> 'Future[_T]':
		return self.__helper.submit(fn, *args, **kwargs)

	@override
	def __exit__(self, exc_type, exc_val, exc_tb):
		return self.__helper.__exit__(exc_type, exc_val, exc_tb)

	def wait_and_ensure_no_error(self):
		self.__helper.wait_and_ensure_no_error()
