import collections
import dataclasses
import functools
import threading
import time
from typing import TypeVar, Generic, Callable, Any, cast

from typing_extensions import ParamSpec

_K = TypeVar('_K')
_V = TypeVar('_V')
_P = ParamSpec('_P')


@dataclasses.dataclass(frozen=True)
class _CacheItem(Generic[_V]):
	value: _V
	expire_at: float


class TTLLRUCache(Generic[_K, _V]):
	def __init__(self, capacity: int, ttl: float):
		self.__capacity = capacity
		self.__ttl = ttl
		self.__data: 'collections.OrderedDict[_K, _CacheItem]' = collections.OrderedDict()
		self.__lock = threading.Lock()

	def get(self, key: _K, default=None) -> _V:
		with self.__lock:
			if key in self.__data:
				cache_item = self.__data[key]
				current_time = time.time()
				if current_time < cache_item.expire_at:
					self.__data.move_to_end(key)
					return cache_item.value
				else:
					self.__data.pop(key)
		return default

	def set(self, key: _K, value: _V):
		with self.__lock:
			if key in self.__data:
				self.__data[key] = _CacheItem(value, time.time() + self.__ttl)
				self.__data.move_to_end(key)
			else:
				if len(self.__data) >= self.__capacity:
					for k in list(self.__data.keys()):
						if time.time() >= self.__data[k].expire_at:
							self.__data.pop(k)
					if len(self.__data) >= self.__capacity:
						self.__data.popitem(last=False)
				self.__data[key] = _CacheItem(value, time.time() + self.__ttl)

	def has(self, key: _K) -> bool:
		return key in self.__data

	def clear(self):
		self.__data.clear()

	def __len__(self) -> int:
		return len(self.__data)

	def __contains__(self, key: _K) -> bool:
		return self.has(key)

	def wrap(self, func: Callable[_P, _V]) -> Callable[_P, _V]:
		# it's not possible to ensure _P.args == _K with python typing :(

		not_found = object()
		lock = threading.Lock()

		@functools.wraps(func)
		def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _V:
			lru_key = cast(_K, args)
			with lock:
				result = self.get(lru_key, default=not_found)
			if result is not not_found:
				return result
			else:
				result = func(*args, **kwargs)
				with lock:
					self.set(lru_key, result)
				return result

		wrapper.cache = self  # type: ignore[attr-defined]
		return wrapper

	def prune_all(self):
		with self.__lock:
			for k in list(self.__data.keys()):
				if time.time() >= self.__data[k].expire_at:
					self.__data.pop(k)


def ttl_lru_cache(capacity: int, ttl: float) -> Callable[[Callable[_P, _V]], Callable[_P, _V]]:
	def decorator(func: Callable[_P, _V]) -> Callable[_P, _V]:
		cache: TTLLRUCache[Any, _V] = TTLLRUCache(capacity, ttl)
		return cache.wrap(func)
	return decorator


class TTLLRUCounter(Generic[_K]):
	def __init__(self, capacity: int, ttl: float):
		self.__lock = threading.Lock()
		self.cache: TTLLRUCache[_K, int] = TTLLRUCache(capacity, ttl)

	def inc(self, key: _K) -> int:
		with self.__lock:
			cnt = self.cache.get(key, default=0)
			self.cache.set(key, cnt + 1)
		return cnt
