import functools
import re
from abc import ABC, abstractmethod
from typing import Union, Tuple, NamedTuple, Generic, Dict, TypeVar

from prime_backup.utils import misc_utils

_T = TypeVar('_T')


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


class UnitValuePair(NamedTuple):
	value: float
	unit: str

	def to_str(self, ndigits: int = 2) -> str:
		return f'{round(self.value, ndigits)}{self.unit}'


class _UnitValueBase(Generic[_T], str, ABC):
	_value: _T

	@classmethod
	@abstractmethod
	def _get_unit_map(cls) -> Dict[str, _T]:
		...

	@classmethod
	@functools.lru_cache
	def __get_unit_map_lowered(cls) -> Dict[str, _T]:
		return {k.lower(): v for k, v in cls._get_unit_map().items()}

	@classmethod
	def parse_unit(cls, unit: str) -> _T:
		ret = cls.__get_unit_map_lowered().get(unit.lower())
		if ret is None:
			raise ValueError('unknown unit {!r}'.format(unit))
		return ret

	@property
	def value(self) -> _T:
		return self._value

	@classmethod
	def _auto_format(cls, val: _T) -> UnitValuePair:
		ret = None
		for unit, k in cls._get_unit_map().items():
			if val >= k or ret is None:
				ret = UnitValuePair(val / k, unit)
			else:
				break
		if ret is None:
			raise AssertionError()
		return ret

	def auto_format(self) -> UnitValuePair:
		return self._auto_format(self._value)

	def __str__(self) -> str:
		return self.auto_format().to_str()

	def __repr__(self) -> str:
		return misc_utils.represent(self, attrs={'value': self._value})


class Duration(_UnitValueBase[float]):
	_value: Union[float, int]
	"""duration in seconds"""

	@classmethod
	@functools.lru_cache
	def _get_unit_map(cls) -> Dict[str, float]:
		data = {
			('ms',): 1e-3,
			('s', 'sec'): 1,
			('m', 'min'): 60,
			('h', 'hour'): 60 * 60,
			('d', 'day'): 60 * 60 * 24,
			('month',): 60 * 60 * 24 * 30,
			('y', 'year'): 60 * 60 * 24 * 365,
		}
		ret = {}
		for units, v in data.items():
			for k in units:
				ret[k] = v
		return ret

	def __new__(cls, s: Union[int, float, str]):
		if isinstance(s, str):
			value, unit = _split_unit(s)
			duration = value * cls.parse_unit(unit)
			obj = super().__new__(cls, s)
			obj._value = duration
		elif isinstance(s, (float, int)):
			obj = super().__new__(cls, cls._auto_format(s).to_str())
			obj._value = s
		else:
			raise TypeError(type(s))
		return obj

	@property
	def value(self) -> Union[float, int]:
		"""
		Duration in second
		"""
		return super().value


class Quantity(_UnitValueBase[Union[float, int]]):
	_bsi = {'': 1, 'Ki': 2 ** 10, 'Mi': 2 ** 20, 'Gi': 2 ** 30, 'Ti': 2 ** 40, 'Pi': 2 ** 50, 'Ei': 2 ** 60}
	_dsi = {'': 1E0, 'K': 1E3, 'M': 1E6, 'G': 1E9, 'T': 1E12, 'P': 1E15, 'E': 1E18}

	@classmethod
	@functools.lru_cache
	def _get_unit_map(cls) -> Dict[str, float]:
		si = cls._bsi.copy()
		for k, v in cls._dsi.items():
			si[k] = int(v)
		return {k: v for k, v in si.items()}

	def __new__(cls, s: Union[int, float, str]):
		if isinstance(s, str):
			value, unit = _split_unit(s)
			value = value * cls.parse_unit(unit)
		elif isinstance(s, int):
			value = int(s)
		elif isinstance(s, float):
			value = s
		else:
			raise TypeError(type(s))

		s = cls._auto_format(value).to_str()
		obj = super().__new__(cls, s)
		obj._value = value
		return obj

	@property
	def value(self) -> Union[int, float]:
		"""
		Byte count
		"""
		return super().value


class ByteCount(Quantity):
	def __new__(cls, s: Union[int, float, str]):
		if isinstance(s, str) and len(s) > 0 and s[-1].lower() == 'b':
			s = s[:-1]
		return super().__new__(cls, s)

	@classmethod
	def _auto_format(cls, val) -> UnitValuePair:
		uv = super()._auto_format(val)
		return UnitValuePair(uv.value, uv.unit + 'B')
