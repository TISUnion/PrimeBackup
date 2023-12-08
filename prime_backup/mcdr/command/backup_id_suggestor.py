import threading
import time
from typing import List, Optional

from mcdreforged.api.all import *

from prime_backup import logger
from prime_backup.mcdr.task.backup.get_backup_ids_task import GetBackupIdsTask
from prime_backup.mcdr.task_manager import TaskManager
from prime_backup.mcdr.task_queue import TaskQueue
from prime_backup.utils.waitable_value import WaitableValue


class BackupIdSuggestor:
	def __init__(self, task_manager: TaskManager):
		self.task_manager = task_manager
		self.logger = logger.get()
		self.lock = threading.Lock()
		self.last_future: WaitableValue[List[int]] = WaitableValue()
		self.last_future.set([])
		self.current_future: WaitableValue[List[int]] = WaitableValue()
		self.current_future.set([])
		self.last_fetch_time = 0

	def request(self, source: CommandSource) -> WaitableValue[List[int]]:
		with self.lock:
			if not self.current_future.is_set():
				return self.last_future

			now = time.time()
			if now - self.last_fetch_time <= 1:  # max qps == 1
				return self.current_future

			def callback(result: Optional[List[int]], err: Optional[Exception]):
				if isinstance(result, list):
					wv.set(result)
				else:
					self.logger.warning('[{}] Backup id list fetched failed: {}'.format(self.__class__.__name__, err))
					wv.set([])

			self.last_fetch_time = now
			wv = WaitableValue()
			try:
				self.task_manager.add_task(GetBackupIdsTask(source), callback, handle_tmo_err=False)
			except TaskQueue.TooManyOngoingTask:
				return self.last_future
			else:
				self.last_future = self.current_future
				self.current_future = wv
				return wv
