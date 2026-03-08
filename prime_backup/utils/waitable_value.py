import threading
from typing import TypeVar, Generic, Union, Optional

from typing_extensions import overload

_T = TypeVar('_T')


class _Empty:
	pass


class WaitableValue(Generic[_T]):
	"""
	A simple version of "Future" that never has exception
	"""

	EMPTY = _Empty()

	def __init__(self):
		self.__lock = threading.Lock()
		self.__condition = threading.Condition(self.__lock)
		self.__value: Union[_T, _Empty] = self.EMPTY

	def get(self) -> _T:
		with self.__lock:
			if isinstance(self.__value, _Empty):
				raise ValueError('value is unset')
			return self.__value

	def set(self, value: _T):
		with self.__condition:
			self.__value = value
			self.__condition.notify_all()

	def __is_set_no_lock(self) -> bool:
		return not isinstance(self.__value, _Empty)

	def is_set(self) -> bool:
		with self.__lock:
			return self.__is_set_no_lock()

	@overload
	def wait(self) -> _T: ...
	@overload
	def wait(self, timeout: float) -> Union[_T, _Empty]: ...

	def wait(self, timeout: Optional[float] = None) -> Union[_T, _Empty]:
		with self.__condition:
			if timeout is None:
				while not self.__is_set_no_lock():
					self.__condition.wait()
			else:
				if not self.__condition.wait_for(self.__is_set_no_lock, timeout):
					return self.EMPTY

			return self.__value

	def clear(self):
		with self.__lock:
			self.__value = self.EMPTY

	def __str__(self):
		with self.__lock:
			if self.__is_set_no_lock():
				return 'WaitableValue[value={}]'.format(self.__value)
			else:
				return 'WaitableValue[empty]'
