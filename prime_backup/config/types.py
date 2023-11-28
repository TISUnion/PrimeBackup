import enum
import functools
import importlib
import re
from typing import Dict, Tuple, Union, NamedTuple, Protocol

from prime_backup.utils import misc_utils


class Hasher(Protocol):
	def update(self, b: bytes):
		...

	def hexdigest(self) -> str:
		...


class _HashMethodItem(NamedTuple):
	hasher_func: str
	hex_length: int

	def create_hasher(self) -> Hasher:
		mod_name, func_name = self.hasher_func.split('.')
		mod = importlib.import_module(mod_name)
		func = getattr(mod, func_name)
		return func()


class HashMethod(enum.Enum):
	xxh128 = _HashMethodItem('xxhash.xxh128', 32)
	sha256 = _HashMethodItem('hashlib.sha256', 64)
	blake3 = _HashMethodItem('blake3.blake3', 64)


def _parse_number(s: str) -> Union[int, float]:
	try:
		value = int(s)
	except ValueError:
		try:
			value = float(s)
		except ValueError:
			raise ValueError('{!r} is not a number'.format(s)) from None
		if value.is_integer():
			value = round(value)
	return value


def _split_unit(s: str) -> Tuple[float, str]:
	match = re.fullmatch(r'([-+.\d]+)(.*)', s)
	if not match:
		raise ValueError('bad value {!r}'.format(s))
	return _parse_number(match.group(1)), match.group(2)


class Duration:
	__unit_map_cache = {}

	@classmethod
	@functools.lru_cache
	def __get_unit_map(cls) -> Dict[str, float]:
		data = {
			('ms',): 1e-3,
			('s', 'sec'): 1,
			('m', 'min'): 60,
			('h', 'hour'): 60 * 60,
			('d', 'day'): 60 * 60 * 24,
		}
		ret = {}
		for units, v in data.items():
			for k in units:
				ret[k] = v
		return ret

	@classmethod
	def parse_unit(cls, unit: str) -> float:
		ret = cls.__get_unit_map().get(unit.lower())
		if ret is None:
			raise ValueError('unknown unit {!r}'.format(unit))
		return ret

	def __init__(self, s: str):
		value, unit = _split_unit(s)
		self.__duration: float = value * self.parse_unit(unit)

	@property
	def duration(self) -> float:
		"""
		Duration in second
		"""
		return self.__duration

	def __repr__(self) -> str:
		return misc_utils.represent(self, attrs={'duration': self.__duration})


class Quantity:
	_bsi = {'': 1, 'Ki': 2 ** 10, 'Mi': 2 ** 20, 'Gi': 2 ** 30, 'Ti': 2 ** 40, 'Pi': 2 ** 50, 'Ei': 2 ** 60}
	_dsi = {'': 1E0, 'k': 1E3, 'M': 1E6, 'G': 1E9, 'T': 1E12, 'P': 1E15, 'E': 1E18}

	@classmethod
	@functools.lru_cache
	def __get_si_map(cls) -> Dict[str, int]:
		si = cls._bsi.copy()
		for k, v in cls._dsi.items():
			si[k] = int(v)
		return {k.lower(): v for k, v in si.items()}

	@classmethod
	def parse_si(cls, unit: str) -> int:
		ret = cls.__get_si_map().get(unit.lower())
		if ret is None:
			raise ValueError('unknown unit {!r}'.format(unit))
		return ret

	def __init__(self, s: Union[int, float, str]):
		if isinstance(s, str):
			value, unit = _split_unit(s)
			self.__value = value * self.parse_si(unit)
		elif isinstance(s, int):
			self.__value = int(s)
		elif isinstance(s, float):
			self.__value = s
		else:
			raise TypeError()

	@property
	def value(self) -> Union[int, float]:
		return self.__value

	def auto_format(self) -> Tuple[float, str]:
		if self.__value == 0:
			return 0, ''
		ret = None
		for unit, k in self._bsi.items():
			if self.__value >= k:
				ret = self.__value / k, unit
			else:
				break
		if ret is None:
			raise AssertionError()
		return ret

	def __str__(self) -> str:
		if self.__value < 0:
			return '-' + str(type(self)(-self.__value))
		number, unit = self.auto_format()
		return f'{round(number, 2)}{unit}'

	def __repr__(self) -> str:
		return misc_utils.represent(self, attrs={'value': self.value})


class ByteCount(Quantity):
	def __init__(self, s: Union[int, float, str]):
		if isinstance(s, str) and len(s) > 0 and s[-1].lower() == 'b':
			s = s[:-1]
		super().__init__(s)

	def auto_format(self) -> Tuple[float, str]:
		number, unit = super().auto_format()
		return number, unit + 'B'
