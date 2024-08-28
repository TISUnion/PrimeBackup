import functools
import json
import re
import unittest
from abc import ABC, abstractmethod
from typing import Union, Tuple, Generic, Dict, TypeVar, NamedTuple

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
	match = re.fullmatch(r'([-+.\d]+)(\w*)', s)
	if not match:
		raise ValueError('bad value {!r}'.format(s))
	return _parse_number(match.group(1)), match.group(2)


class ValueUnitPair(NamedTuple):
	value: float
	unit: str

	def to_str(self, ndigits: int = 2, always_sign: bool = False) -> str:
		if ndigits >= 0:
			s = f'{self.value:.{ndigits}f}{self.unit}'
		else:
			s = f'{self.value}{self.unit}'
		if always_sign and s[:1] != '-':
			s = '+' + s
		return s


class _UnitValueBase(Generic[_T], str, ABC):
	_value: _T

	@classmethod
	@abstractmethod
	def _get_unit_map(cls) -> Dict[str, _T]:
		...

	@classmethod
	def _get_formatting_unit_map(cls) -> Dict[str, _T]:
		return cls._get_unit_map()

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

	@staticmethod
	def __precise_div(a: Union[float, int], b: Union[float, int]) -> Union[float, int]:
		if isinstance(a, int) and (1 / b).is_integer():
			return a * int(1 / b)
		return a / b

	@classmethod
	def _auto_format(cls, val: _T) -> ValueUnitPair:
		if val < 0:
			uvp = cls._auto_format(-val)
			return ValueUnitPair(-uvp.value, uvp.unit)
		ret = None
		for unit, k in cls._get_formatting_unit_map().items():
			x = cls.__precise_div(val, k)
			if x >= 1 or ret is None:
				if isinstance(x, float) and x.is_integer():
					x = int(x)
				ret = ValueUnitPair(x, unit)
			else:
				break
		if ret is None:
			raise AssertionError()
		return ret

	@classmethod
	def _precise_format(cls, val: _T) -> ValueUnitPair:
		if val < 0:
			uvp = cls._auto_format(-val)
			return ValueUnitPair(-uvp.value, uvp.unit)

		units = list(reversed(cls._get_formatting_unit_map().items()))
		if val == 0:
			return ValueUnitPair(val, units[-1][0])
		for i, tp in enumerate(units):  # high -> low
			unit, k = tp
			x = cls.__precise_div(val, k)
			if isinstance(x, int) or (isinstance(x, float) and x.is_integer()) or i == len(units) - 1:
				if isinstance(x, float) and x.is_integer():
					x = int(x)
				return ValueUnitPair(x, unit)
		raise AssertionError()

	def precise_format(self) -> ValueUnitPair:
		return self._precise_format(self._value)

	def auto_format(self) -> ValueUnitPair:
		return self._auto_format(self._value)

	def auto_str(self, **kwargs) -> str:
		return self.auto_format().to_str(**kwargs)

	def precise_str(self, **kwargs) -> str:
		return self.precise_format().to_str(**kwargs)

	def __str__(self) -> str:
		return self.precise_str(ndigits=-1)

	def __repr__(self) -> str:
		return misc_utils.represent(self, attrs={'value': self._value})


class Duration(_UnitValueBase[float]):
	_value: Union[float, int]
	"""duration in seconds"""

	__units = {
		('ms',): 1e-3,
		('s', 'sec'): 1,
		('m', 'min'): 60,
		('h', 'hour'): 60 * 60,
		('d', 'day'): 60 * 60 * 24,
		('mon', 'month'): 60 * 60 * 24 * 30,
		('y', 'year'): 60 * 60 * 24 * 365,
	}

	@classmethod
	@functools.lru_cache
	def _get_unit_map(cls) -> Dict[str, float]:
		ret = {}
		for units, v in cls.__units.items():
			for k in units:
				ret[k] = v
		return ret

	@classmethod
	@functools.lru_cache
	def _get_formatting_unit_map(cls) -> Dict[str, float]:
		ret = {}
		for u in ['s', 'm', 'h', 'd']:
			ret[u] = cls._get_unit_map()[u]
		return ret

	def __new__(cls, s: Union[int, float, str]):
		if isinstance(s, str):
			value, unit = _split_unit(s)
			duration = value * cls.parse_unit(unit)
			if isinstance(duration, float) and duration.is_integer():
				duration = int(duration)
			obj = super().__new__(cls, s)
			obj._value = duration
		elif isinstance(s, (float, int)):
			obj = super().__new__(cls, cls._precise_format(s).to_str(ndigits=-1))
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

	@property
	def value_nano(self) -> Union[float, int]:
		"""
		Duration in nanosecond
		"""
		return self.value * 10 ** 9


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

		obj = super().__new__(cls, cls._precise_format(value).to_str(ndigits=-1))
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
	def _auto_format(cls, val) -> ValueUnitPair:
		uv = super()._auto_format(val)
		if not uv.unit.endswith('B'):
			uv = ValueUnitPair(uv.value, uv.unit + 'B')
		return uv

	@classmethod
	def _precise_format(cls, val) -> ValueUnitPair:
		uv = super()._precise_format(val)
		if not uv.unit.endswith('B'):
			uv = ValueUnitPair(uv.value, uv.unit + 'B')
		return uv


class UnitTests(unittest.TestCase):
	def test_1_types(self):
		for cls in [Duration, Quantity, ByteCount]:
			for val in [0, '18', 127, 1024, 1440]:
				inst = cls(val + 's' if cls == Duration and isinstance(val, str) else val)
				self.assertEqual(cls, type(inst))
				self.assertIsInstance(inst, str)
				self.assertEqual(int(val), getattr(inst, 'value'))

	def test_2_1_duration_format(self):
		self.assertEqual(123, Duration(123).value)
		self.assertEqual(123, Duration('123s').value)
		self.assertEqual(ValueUnitPair(2.05, 'm'), Duration('123s').auto_format())
		self.assertEqual(ValueUnitPair(123, 's'), Duration('123sec').precise_format())

		self.assertEqual(1440, Duration(1440).value)
		self.assertEqual('24m', str(Duration('1440s')))
		self.assertEqual(ValueUnitPair(24, 'm'), Duration('1440s').auto_format())
		self.assertEqual(ValueUnitPair(24, 'm'), Duration('1440s').precise_format())

		self.assertEqual(12.3, Duration(12.3).value)
		self.assertEqual(12.3, Duration('12.3s').value)
		self.assertEqual(ValueUnitPair(12.3, 's'), Duration('12.3s').auto_format())
		self.assertEqual(ValueUnitPair(12.3, 's'), Duration('12.3s').precise_format())

		self.assertEqual(1234.5678, Duration(1234.5678).value)
		self.assertEqual(1234.5678, Duration('1234.5678s').value)
		self.assertEqual(ValueUnitPair(1234.5678 / 60, 'm'), Duration('1234.5678s').auto_format())
		self.assertEqual(ValueUnitPair(1234.5678, 's'), Duration('1234.5678s').precise_format())

	def test_2_2_quantity_format(self):
		self.assertEqual(1234, Quantity(1234).value)
		self.assertEqual(1234, Quantity('1234').value)
		self.assertEqual(ValueUnitPair(1234 / 1024, 'Ki'), Quantity('1234').auto_format())
		self.assertEqual(ValueUnitPair(1234, ''), Quantity('1234').precise_format())

		self.assertEqual(4096, Quantity(4096).value)
		self.assertEqual('4Ki', str(Quantity('4096')))
		self.assertEqual(ValueUnitPair(4, 'Ki'), Quantity('4096').auto_format())
		self.assertEqual(ValueUnitPair(4, 'Ki'), Quantity('4096').precise_format())

	def test_2_3_byte_count_format(self):
		self.assertEqual(1234, ByteCount(1234).value)
		self.assertEqual(1234, ByteCount('1234').value)
		self.assertEqual('4KiB', str(ByteCount('4096')))

	def test_3_convert(self):
		from mcdreforged.api.utils import serializer
		for cls in [Duration, Quantity, ByteCount]:
			vals = [0, 127, 1024, 1440]
			if cls in [Duration]:
				vals.extend(['0s', '18s', '36m'])
			else:
				vals += ['2Gi', '3M', '4ki']
			for val in vals:
				a = cls(val)
				self.assertEqual(str(a), serializer.serialize(a))

				b = serializer.deserialize(serializer.serialize(a), cls)
				self.assertEqual(a.value, b.value)

				c = json.loads(json.dumps(a))
				self.assertEqual(str(a), c)


if __name__ == '__main__':
	unittest.main()

