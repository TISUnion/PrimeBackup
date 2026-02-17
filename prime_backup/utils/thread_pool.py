import functools
import queue
import threading
from concurrent.futures import ThreadPoolExecutor, Future
from typing import List

from typing_extensions import override

from prime_backup.utils import misc_utils


class FailFastBlockingThreadPool(ThreadPoolExecutor):
	"""
	A thread pool that:
	1. submit() calls will be blocked if there's no free worker
	2. makes exception raise as soon as possible
	3. no more task will be submitted after an exception raises
	"""
	def __init__(self, name: str):
		from prime_backup.config.config import Config
		thread_name_prefix = misc_utils.make_thread_name(name)
		max_workers: int = Config.get().get_effective_concurrency()

		super().__init__(max_workers=max_workers, thread_name_prefix=thread_name_prefix)
		self.__sem = threading.Semaphore(max_workers)
		self.__all_futures: List[Future] = []
		self.__error_futures: 'queue.Queue[Future]' = queue.Queue()

	@override
	def submit(self, __fn, *args, **kwargs):
		func = functools.partial(__fn, *args, **kwargs)

		def wrapper_func():
			try:
				return func()
			except Exception:
				self.__error_futures.put(future)
				raise
			finally:
				self.__sem.release()

		self.__sem.acquire()
		try:
			f = self.__error_futures.get(block=False)
		except queue.Empty:
			pass
		else:
			f.result()

		future = super().submit(wrapper_func)
		self.__all_futures.append(future)
		return future

	@override
	def __exit__(self, exc_type, exc_val, exc_tb):
		if exc_type is None:
			# check task exception if no error occurs
			self.wait_and_ensure_no_error()

		super().__exit__(exc_type, exc_val, exc_tb)

	def wait_and_ensure_no_error(self):
		for future in self.__all_futures:
			future.result()
