import collections
import contextlib
import operator
import time
from typing import Dict, TypeVar, Generic, Optional, Callable, Any, Generator

_K = TypeVar('_K')


class TimeCostStats(Generic[_K]):
	def __init__(self):
		self.__costs: Dict[_K, float] = collections.defaultdict(float)

	@contextlib.contextmanager
	def measure_time_cost(self, *keys: _K) -> Generator[Callable[[], float], Any, None]:
		def get_cost() -> float:
			if cost is None:
				raise RuntimeError('not done yet')
			return cost

		cost: Optional[float] = None
		start = time.time()
		try:
			yield get_cost
		finally:
			cost = time.time() - start
			for key in keys:
				self.__costs[key] += cost

	def get_cost(self, key: _K) -> float:
		return self.__costs.get(key, 0)

	def get_costs(self, *, by_key: bool = False, by_cost: bool = False) -> Dict[_K, float]:
		if by_key:
			idx = 0
		elif by_cost:
			idx = 1
		else:
			return self.__costs.copy()
		return dict(sorted(self.__costs.items(), key=operator.itemgetter(idx)))

	def reset(self):
		self.__costs.clear()
