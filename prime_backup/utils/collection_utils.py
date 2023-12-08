from typing import TypeVar, Generator, List

T = TypeVar('T')
GSlice = Generator[T, None, None]


def slice_view(lst: List[T], start: int, end: int) -> GSlice:
	"""
	equals to `lst[start:end]`, range: [start, end)
	"""
	for i in range(start, end):
		yield lst[i]


def slicing_iterate(lst: List[T], chunk_size: int) -> Generator[GSlice, None, None]:
	for i in range(0, len(lst), chunk_size):
		yield slice_view(lst, i, min(i + chunk_size, len(lst)))


def deduplicated_list(lst: List[T]) -> List[T]:
	return list(dict.fromkeys(lst).keys())
