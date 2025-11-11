import dataclasses
import operator
import queue
from typing import TypeVar, Generator, List, Iterator, Generic, Dict, Callable, Iterable

_T = TypeVar('_T')
_K = TypeVar('_K')
_V = TypeVar('_V')


def slicing_iterate(lst: List[_T], chunk_size: int) -> Generator[List[_T], None, None]:
	for i in range(0, len(lst), chunk_size):
		yield lst[i:min(i + chunk_size, len(lst))]


def deduplicated_list(lst: Iterable[_T]) -> List[_T]:
	if isinstance(lst, set):
		return list(lst)
	return list(dict.fromkeys(lst).keys())


def drain_queue(q: 'queue.Queue[_T]') -> Iterator[_T]:
	while True:
		try:
			yield q.get(block=False)
		except queue.Empty:
			break


@dataclasses.dataclass(frozen=True)
class DictValueDelta(Generic[_T]):
	@dataclasses.dataclass(frozen=True)
	class OldNewValue(Generic[_V]):
		old: _V
		new: _V

	added: List[_T] = dataclasses.field(default_factory=list)  # list of new
	removed: List[_T] = dataclasses.field(default_factory=list)  # list of old
	changed: List[OldNewValue[_T]] = dataclasses.field(default_factory=list)  # list of (old, new)

	def size(self) -> int:
		return len(self.added) + len(self.removed) + len(self.changed)


def compute_dict_value_delta(old_dict: Dict[_K, _V], new_dict: Dict[_K, _V], *, cmp: Callable[[_V, _V], bool] = operator.eq) -> DictValueDelta[_V]:
	delta: DictValueDelta[_V] = DictValueDelta()
	for key, old_value in old_dict.items():
		if key in new_dict:
			new_value = new_dict[key]
			if not cmp(new_value, old_value):
				delta.changed.append(DictValueDelta.OldNewValue(old_value, new_value))
		else:
			delta.removed.append(old_value)
	for key in new_dict.keys():
		if key not in old_dict:
			delta.added.append(new_dict[key])
	return delta


if __name__ == '__main__':
	for view in slicing_iterate(list(range(100)), 8):
		print(view)
