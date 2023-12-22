from typing import Union, Callable, Any, Optional, Type, Tuple, TypeVar

T = TypeVar('T')


def assert_true(expr: bool, msg: Union[str, Callable[[], str]]):
	if not expr:
		if callable(msg):
			msg = msg()
		raise AssertionError(msg)


def represent(obj: Any, *, attrs: Optional[dict] = None) -> str:
	if attrs is None:
		attrs = {name: value for name, value in vars(obj).items() if not name.startswith('_')}
	kv = []
	for name, value in attrs.items():
		kv.append(f'{name}={value}')
	return '{}({})'.format(type(obj).__name__, ', '.join(kv))


def ensure_type(value: Any, class_or_tuple: Union[Tuple[Type[T]], Type[T], Type]) -> T:
	if not isinstance(value, class_or_tuple):
		raise TypeError('bad type {}, should be {}'.format(type(value), class_or_tuple))
	return value


def make_thread_name(name: str) -> str:
	from prime_backup import constants
	return f'PB@{constants.INSTANCE_ID}-{name}'
