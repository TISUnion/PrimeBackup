from typing import Union, Callable, Any, Optional, Type, Tuple, TypeVar

from typing_extensions import overload

from prime_backup.constants import constants

_T = TypeVar('_T')
_T1 = TypeVar('_T1')
_T2 = TypeVar('_T2')
_T3 = TypeVar('_T3')


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


@overload
def ensure_type(value: Any, types: Type[_T]) -> _T:
	...


@overload
def ensure_type(value: Any, types: None) -> None:
	...


@overload
def ensure_type(value: Any, types: Tuple[Type[_T1], Type[_T2]]) -> Union[_T1, _T2]:
	...


@overload
def ensure_type(value: Any, types: Tuple[None, Type[_T2]]) -> Union[None, _T2]:
	...


@overload
def ensure_type(value: Any, types: Tuple[Type[_T1], None]) -> Union[_T1, None]:
	...


def ensure_type(value: Any, types: Any) -> Any:
	if not isinstance(value, types):
		raise TypeError('bad type {}, should be {}'.format(type(value), types))
	return value


def make_thread_name(name: str) -> str:
	return f'PB@{constants.INSTANCE_ID}-{name}'
