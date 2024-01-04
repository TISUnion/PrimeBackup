import functools
import queue
import threading
from concurrent.futures import ThreadPoolExecutor, Future

from prime_backup.utils import misc_utils


class FailFastThreadPool(ThreadPoolExecutor):
	"""
	A thread pool that:
	- makes exception raise as soon as possible
	- no more task will be submitted after an exception raises
	"""
	def __init__(self, name: str):
		from prime_backup.config.config import Config
		thread_name_prefix = misc_utils.make_thread_name(name)
		max_workers: int = Config.get().get_effective_concurrency()

		super().__init__(max_workers=max_workers, thread_name_prefix=thread_name_prefix)
		self.__sem = threading.Semaphore(max_workers)
		self.__all_futures: 'queue.Queue[Future]' = queue.Queue()
		self.__error_futures: 'queue.Queue[Future]' = queue.Queue()

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
		self.__all_futures.put(future)
		return future

	def __exit__(self, exc_type, exc_val, exc_tb):
		if exc_type is None:
			# check task exception if no error occurs
			from prime_backup.utils import collection_utils
			for future in collection_utils.drain_queue(self.__all_futures):
				future.result()

		super().__exit__(exc_type, exc_val, exc_tb)
