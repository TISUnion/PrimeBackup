import queue
from typing import TypeVar, Generator, List, Iterator

T = TypeVar('T')
_GSlice = Generator[T, None, None]


def slice_view(lst: List[T], start: int, end: int) -> _GSlice:
	"""
	equals to `lst[start:end]`, range: [start, end)
	"""
	for i in range(start, end):
		yield lst[i]


def slicing_iterate(lst: List[T], chunk_size: int) -> Generator[_GSlice, None, None]:
	for i in range(0, len(lst), chunk_size):
		yield slice_view(lst, i, min(i + chunk_size, len(lst)))


def deduplicated_list(lst: List[T]) -> List[T]:
	return list(dict.fromkeys(lst).keys())


def drain_queue(q: 'queue.Queue[T]') -> Iterator[T]:
	while True:
		try:
			yield q.get(block=False)
		except queue.Empty:
			break
