import threading
from typing import TypeVar, Generic, Union

_T = TypeVar('_T')


class _Empty:
	pass


class WaitableValue(Generic[_T]):
	EMPTY = _Empty()

	def __init__(self):
		self.__event = threading.Event()
		self.__value = self.EMPTY

	def get(self) -> _T:
		if not self.__event.is_set():
			raise ValueError('value unset')
		return self.__value

	def set(self, value: _T):
		self.__value = value
		self.__event.set()

	def is_set(self) -> bool:
		return self.__event.is_set()

	def wait(self, timeout: float = None) -> Union[_T, _Empty]:
		if self.__event.wait(timeout):
			return self.get()
		else:
			return self.EMPTY

	def __str__(self):
		if self.is_set():
			return 'WaitableValue[value={}]'.format(self.__value)
		else:
			return 'WaitableValue[empty]'
