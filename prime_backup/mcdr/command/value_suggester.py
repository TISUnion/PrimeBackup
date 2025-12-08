import threading
import time
from abc import abstractmethod, ABC
from typing import Optional, Generic, TypeVar, List

from mcdreforged.api.all import CommandSource, CommandContext
from typing_extensions import override

from prime_backup import logger
from prime_backup.mcdr.task.backup.get_backup_ids_task import GetBackupIdsTask
from prime_backup.mcdr.task.basic_task import LightTask
from prime_backup.mcdr.task.db.get_backup_file_paths_task import GetBackupFilePathsTask
from prime_backup.mcdr.task.db.get_fileset_file_paths_task import GetFilesetFilePathsTask
from prime_backup.mcdr.task.db.get_fileset_ids_task import GetFilesetIdsTask
from prime_backup.mcdr.task_manager import TaskManager
from prime_backup.mcdr.task_queue import TaskQueue
from prime_backup.utils import misc_utils
from prime_backup.utils.waitable_value import WaitableValue

_T = TypeVar('_T')
_P = TypeVar('_P')


class ValueSuggestor(ABC, Generic[_T, _P]):
	def __init__(self, task_manager: TaskManager):
		self.task_manager = task_manager
		self.logger = logger.get()
		self.lock = threading.Lock()
		self.last_future: WaitableValue[_T] = WaitableValue()
		self.current_future: WaitableValue[_T] = WaitableValue()
		self.current_task_key: str = ''
		self.last_fetch_time = 0
		self.__reset()

	@abstractmethod
	def _create_fallback(self) -> _T:
		...

	@abstractmethod
	def _create_value_task(self, source: CommandSource, arg: _P) -> LightTask[_T]:
		...

	@abstractmethod
	def _compute_task_key(self, source: CommandSource, arg: _P) -> str:
		...

	def __reset(self):
		self.last_future.set(self._create_fallback())
		self.current_future.set(self._create_fallback())

	def request(self, source: CommandSource, arg: _P) -> WaitableValue[_T]:
		with self.lock:
			task_key = self._compute_task_key(source, arg)
			if task_key != self.current_task_key:
				self.__reset()
			self.current_task_key = task_key

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
			task = self._create_value_task(source, arg)
			try:
				self.task_manager.add_task(task, callback, handle_tmo_err=False)
			except TaskQueue.TooManyOngoingTask:
				return self.last_future
			else:
				self.last_future = self.current_future
				self.current_future = wv
				return wv

	def suggest(self, source: CommandSource, arg: _P, timeout: float = 0.2) -> _T:
		wv = self.request(source, arg)
		if wv.wait(timeout) == WaitableValue.EMPTY:
			return self._create_fallback()
		return wv.get()


class BackupIdSuggestor(ValueSuggestor[List[int], None]):
	@override
	def _create_fallback(self) -> List[int]:
		return []

	@override
	def _create_value_task(self, source: CommandSource, ctx: CommandContext) -> LightTask[List[int]]:
		return GetBackupIdsTask(source)

	@override
	def _compute_task_key(self, source: CommandSource, ctx: CommandContext) -> str:
		return ''


class FilesetIdSuggestor(ValueSuggestor[List[int], None]):
	@override
	def _create_fallback(self) -> List[int]:
		return []

	@override
	def _create_value_task(self, source: CommandSource, ctx: CommandContext) -> LightTask[List[int]]:
		return GetFilesetIdsTask(source)

	@override
	def _compute_task_key(self, source: CommandSource, ctx: CommandContext) -> str:
		return ''


class BackupFilePathSuggestor(ValueSuggestor[List[str], int]):
	@override
	def _create_fallback(self) -> List[str]:
		return []

	@override
	def _create_value_task(self, source: CommandSource, backup_id_arg: str) -> LightTask[List[str]]:
		return GetBackupFilePathsTask(source, backup_id_arg)

	@override
	def _compute_task_key(self, source: CommandSource, backup_id_arg: str) -> str:
		return backup_id_arg

	@override
	def suggest(self, source: CommandSource, backup_id: int, timeout: float = 0.2) -> _T:
		return super().suggest(source, backup_id, timeout)


class FilesetFilePathSuggestor(ValueSuggestor[List[str], int]):
	@override
	def _create_fallback(self) -> List[str]:
		return []

	@override
	def _create_value_task(self, source: CommandSource, fileset_id: int) -> LightTask[List[str]]:
		return GetFilesetFilePathsTask(source, fileset_id)

	@override
	def _compute_task_key(self, source: CommandSource, fileset_id: int) -> str:
		return str(fileset_id)

	@override
	def suggest(self, source: CommandSource, fileset_id: int, timeout: float = 0.2) -> _T:
		return super().suggest(source, fileset_id, timeout)


class ValueSuggesters:
	def __init__(self, task_manager: TaskManager):
		self.backup_id_suggestor = BackupIdSuggestor(task_manager)
		self.fileset_id_suggestor = FilesetIdSuggestor(task_manager)
		self.backup_file_path_suggestor = BackupFilePathSuggestor(task_manager)
		self.fileset_file_path_suggestor = FilesetFilePathSuggestor(task_manager)

	def suggest_backup_id(self, source: CommandSource) -> List[str]:
		return [str(backup_id) for backup_id in self.backup_id_suggestor.suggest(source, None)]

	def suggest_fileset_id(self, source: CommandSource) -> List[str]:
		return [str(fileset_id) for fileset_id in self.fileset_id_suggestor.suggest(source, None)]

	def suggest_backup_file_path(self, source: CommandSource, backup_id_arg: str) -> List[str]:
		misc_utils.ensure_type(backup_id_arg, str)
		return self.backup_file_path_suggestor.suggest(source, backup_id_arg)

	def suggest_fileset_file_path(self, source: CommandSource, fileset_id: int) -> List[str]:
		misc_utils.ensure_type(fileset_id, int)
		return self.fileset_file_path_suggestor.suggest(source, fileset_id)
