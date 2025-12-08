import threading
import time
from abc import abstractmethod, ABC
from typing import Optional, Generic, TypeVar, List

from mcdreforged.api.all import CommandSource
from typing_extensions import override

from prime_backup import logger
from prime_backup.mcdr.task.backup.get_backup_ids_task import GetBackupIdsTask
from prime_backup.mcdr.task.basic_task import LightTask
from prime_backup.mcdr.task.db.get_fileset_ids_task import GetFilesetIdsTask
from prime_backup.mcdr.task_manager import TaskManager
from prime_backup.mcdr.task_queue import TaskQueue
from prime_backup.utils.waitable_value import WaitableValue

_T = TypeVar('_T')


class ValueSuggestor(ABC, Generic[_T]):
	def __init__(self, task_manager: TaskManager):
		self.task_manager = task_manager
		self.logger = logger.get()
		self.lock = threading.Lock()
		self.last_future: WaitableValue[_T] = WaitableValue()
		self.last_future.set(self._create_fallback())
		self.current_future: WaitableValue[_T] = WaitableValue()
		self.current_future.set(self._create_fallback())
		self.last_fetch_time = 0

	@abstractmethod
	def _create_fallback(self) -> _T:
		...

	@abstractmethod
	def _create_value_task(self, source: CommandSource) -> LightTask[_T]:
		...

	def request(self, source: CommandSource) -> WaitableValue[_T]:
		with self.lock:
			if not self.current_future.is_set():
				return self.last_future

			now = time.time()
			if now - self.last_fetch_time <= 1:  # max qps == 1
				return self.current_future

			def callback(result: Optional[_T], err: Optional[Exception]):
				if err is None and result is not None:
					wv.set(result)
				else:
					self.logger.warning('[{}] Task {} run failed: {}'.format(task, self.__class__.__name__, err))
					wv.set(self._create_fallback())

			self.last_fetch_time = now
			wv = WaitableValue()
			task = self._create_value_task(source)
			try:
				self.task_manager.add_task(task, callback, handle_tmo_err=False)
			except TaskQueue.TooManyOngoingTask:
				return self.last_future
			else:
				self.last_future = self.current_future
				self.current_future = wv
				return wv

	def suggest(self, source: CommandSource, timeout: float = 0.2) -> _T:
		wv = self.request(source)
		if wv.wait(timeout) == WaitableValue.EMPTY:
			return self._create_fallback()
		return wv.get()


class BackupIdSuggestor(ValueSuggestor[List[int]]):
	@override
	def _create_fallback(self) -> List[int]:
		return []

	@override
	def _create_value_task(self, source: CommandSource) -> LightTask[List[int]]:
		return GetBackupIdsTask(source)


class FilesetIdSuggestor(ValueSuggestor[List[int]]):
	@override
	def _create_fallback(self) -> List[int]:
		return []

	@override
	def _create_value_task(self, source: CommandSource) -> LightTask[List[int]]:
		return GetFilesetIdsTask(source)
