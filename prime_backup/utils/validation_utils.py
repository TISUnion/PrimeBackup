from typing import Union, Callable, NoReturn

from prime_backup.utils import misc_utils

INT32_MIN = -2 ** 31
INT32_MAX = 2 ** 31 - 1

INT64_MIN = -2 ** 63
INT64_MAX = 2 ** 63 - 1


MessageSupplier = Union[str, Callable[[], str]]


def __raise(msg: MessageSupplier, detail: str) -> NoReturn:
	if callable(msg):
		msg = msg()
	if msg:
		raise ValueError(f'{msg}: {detail}')
	else:
		raise ValueError(detail)


def validate_int32(value: int, msg: MessageSupplier):
	misc_utils.ensure_type(value, int)
	if not (INT32_MIN <= value <= INT32_MAX):
		__raise(msg, f'value {value} out of range [{INT32_MIN}, {INT32_MAX}]')


def validate_int64(value: int, msg: MessageSupplier):
	misc_utils.ensure_type(value, int)
	if not (INT64_MIN <= value <= INT64_MAX):
		__raise(msg, f'value {value} out of range [{INT64_MIN}, {INT64_MAX}]')
