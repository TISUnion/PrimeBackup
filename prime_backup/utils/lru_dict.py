import collections
import contextlib
from typing import TypeVar, Generic, Union

_K = TypeVar('_K')
_V = TypeVar('_V')
_T = TypeVar('_T')


class LruDict(Generic[_K, _V]):
	def __init__(self, max_size: int):
		self.__max_size = max_size
		self.__dict: 'collections.OrderedDict[_K, _V]' = collections.OrderedDict()

	def __len__(self) -> int:
		return len(self.__dict)

	def set(self, key: _K, value: _V):
		self.__dict[key] = value
		self.__dict.move_to_end(key)

		while len(self) > self.__max_size:
			old_key = next(iter(self.__dict))
			self.__dict.pop(old_key)

	def get(self, key: _K, default: _T) -> Union[_T, _V]:
		ret = self.__dict.get(key, default)
		with contextlib.suppress(KeyError):
			self.__dict.move_to_end(key)
		return ret

	def clear(self):
		self.__dict.clear()
