import threading
from concurrent.futures import ThreadPoolExecutor, Future
from typing import List, TypeVar, Callable, Optional

from typing_extensions import override, ParamSpec

from prime_backup.utils import misc_utils
from prime_backup.utils.run_once import RunOnceFunc

_T = TypeVar('_T')
_P = ParamSpec('_P')


class FailFastBlockingThreadPool(ThreadPoolExecutor):
	"""
	A thread pool that:
	1. submit() calls will be blocked if there's no free worker
	2. makes exception raise as soon as possible
	3. no more task will be submitted after an exception raises
	4. nested submit() from worker threads is unsupported and may deadlock
	"""
	def __init__(self, name: str, max_workers: Optional[int] = None):
		if max_workers is None:
			from prime_backup.config.config import Config
			max_workers = Config.get().get_effective_concurrency()

		thread_name_prefix = misc_utils.make_thread_name(name)
		super().__init__(max_workers=max_workers, thread_name_prefix=thread_name_prefix)
		self.__sem = threading.Semaphore(max_workers)
		self.__all_futures: List[Future] = []
		self.__error_future: Optional[Future] = None
		self.__error_future_lock = threading.Lock()

	def __raise_error_if_any(self):
		# re-raise the worker exception
		with self.__error_future_lock:
			if self.__error_future is not None:
				self.__error_future.result()

	@override
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

			future = super().submit(fn, *args, **kwargs)
			future.add_done_callback(wrapper_done_callback)
			self.__all_futures.append(future)
			return future
		except Exception:
			sem_releaser()
			raise

	@override
	def __exit__(self, exc_type, exc_val, exc_tb):
		if exc_type is None:
			# check task exception if no error occurs
			self.wait_and_ensure_no_error()

		super().__exit__(exc_type, exc_val, exc_tb)

	def wait_and_ensure_no_error(self):
		for future in self.__all_futures:
			future.result()