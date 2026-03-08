import threading
from typing import Generic, Callable

from typing_extensions import ParamSpec

_P = ParamSpec('_P')


class RunOnceFunc(Generic[_P]):
	def __init__(self, func: Callable[_P, None]):
		self.__func = func
		self.__has_run = False
		self.__lock = threading.Lock()

	def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> None:
		return self.run(*args, **kwargs)

	def run(self, *args: _P.args, **kwargs: _P.kwargs) -> None:
		if self.__has_run:
			return

		with self.__lock:
			if self.__has_run:
				return
			try:
				self.__func(*args, **kwargs)
			finally:
				self.__has_run = True
