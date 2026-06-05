import functools
import threading
from typing import Callable, Any, TypeVar

_T = TypeVar('_T')
_NONE = object()


def cached(func: Callable[[], _T]) -> Callable[[], _T]:
	result: Any = _NONE
	lock = threading.Lock()

	@functools.wraps(func)
	def wrapper() -> _T:
		nonlocal result
		if result is _NONE:
			with lock:
				if result is _NONE:
					result = func()
		return result

	return wrapper
