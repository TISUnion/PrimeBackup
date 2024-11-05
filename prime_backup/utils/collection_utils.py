import queue
from typing import TypeVar, Generator, List, Iterator

_T = TypeVar('_T')


def slicing_iterate(lst: List[_T], chunk_size: int) -> Generator[List[_T], None, None]:
	for i in range(0, len(lst), chunk_size):
		yield lst[i:min(i + chunk_size, len(lst))]


def deduplicated_list(lst: List[_T]) -> List[_T]:
	return list(dict.fromkeys(lst).keys())


def drain_queue(q: 'queue.Queue[_T]') -> Iterator[_T]:
	while True:
		try:
			yield q.get(block=False)
		except queue.Empty:
			break


if __name__ == '__main__':
	for view in slicing_iterate(list(range(100)), 8):
		print(view)
