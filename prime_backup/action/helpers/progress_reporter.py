import logging
import threading
import time
from typing import Optional

from prime_backup import logger
from prime_backup.db import schema
from prime_backup.types.units import ByteCount


class SizeProgressReporter:
	def __init__(self, what: str, *, total_count: int, total_size: int, report_interval: float = 30):
		self.logger: logging.Logger = logger.get()
		self.__what = what
		self.__total_count = total_count
		self.__total_size = total_size
		self.__report_interval = report_interval

		self.__done_count = 0
		self.__done_size = 0
		self.__start_time = time.time()
		self.__last_report_time = self.__start_time
		self.__last_report_size = 0
		self.__lock = threading.Lock()

	def on_one_done(self, size: int):
		log_msg: Optional[str] = None

		with self.__lock:
			self.__done_count += 1
			self.__done_size += size

			now = time.time()
			if now - self.__last_report_time > self.__report_interval:
				time_delta = now - self.__last_report_time
				size_delta = self.__done_size - self.__last_report_size
				log_msg = '{} progress: {} / {} done, size {} / {}, speed {}/s, elapsed time {:.2f}s'.format(
					self.__what,
					self.__done_count, self.__total_count,
					ByteCount(self.__done_size).auto_str(), ByteCount(self.__total_size).auto_str(),
					ByteCount(size_delta / time_delta).auto_str() if time_delta > 0 else 0,
					now - self.__start_time,
				)
				self.__last_report_time = now
				self.__last_report_size = self.__done_size

		if log_msg is not None:
			self.logger.info(log_msg)

	def on_one_file_done(self, file: schema.File):
		self.on_one_done(file.blob_raw_size or 0)
