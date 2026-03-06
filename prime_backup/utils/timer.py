import time
from typing import Optional


def _now() -> float:
	return time.time()


class Timer:
	__start_time: float
	__end_time: Optional[float]

	def __init__(self):
		self.start()

	def start(self):
		self.__start_time = _now()
		self.__end_time = None

	def restart(self):
		self.start()

	def stop(self):
		self.__end_time = _now()

	def is_ticking(self) -> bool:
		return self.__end_time is None

	def get_elapsed(self) -> float:
		end_time = self.__end_time or _now()
		return end_time - self.__start_time

	def get_and_restart(self) -> float:
		ret = self.get_elapsed()
		self.restart()
		return ret
