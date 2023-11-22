import functools
import re
from typing import Dict, Tuple, Union


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


class Duration(str):
	__unit_map_cache = {}

	@classmethod
	@functools.lru_cache
	def __get_unit_map(cls) -> Dict[str, float]:
		data = {
			('ms',): 1e-3,
			('', 's', 'sec'): 1,
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


class Quantity(str):
	@classmethod
	@functools.lru_cache
	def __get_si_map(cls) -> Dict[str, int]:
		bsi = {'Ki': 2 ** 10, 'Mi': 2 ** 20, 'Gi': 2 ** 30, 'Ti': 2 ** 40, 'Pi': 2 ** 50, 'Ei': 2 ** 60}
		dsi = {'': 1E0, 'k': 1E3, 'M': 1E6, 'G': 1E9, 'T': 1E12, 'P': 1E15, 'E': 1E18}
		si = bsi
		for k, v in dsi.items():
			si[k] = int(v)
		return {k.lower(): v for k, v in si.items()}

	@classmethod
	def parse_si(cls, unit: str) -> int:
		ret = cls.__get_si_map().get(unit.lower())
		if ret is None:
			raise ValueError('unknown unit {!r}'.format(unit))
		return ret

	def __init__(self, s: str):
		value, unit = _split_unit(s)
		self.__value = value * self.parse_si(unit)

	@property
	def value(self) -> Union[int, float]:
		return self.__value


class ByteCount(Quantity):
	def __init__(self, s: str):
		if len(s) > 0 and s[-1].lower() == 'b':
			s = s[:-1]
		super().__init__(s)
