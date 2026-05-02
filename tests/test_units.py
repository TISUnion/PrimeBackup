import json
import unittest
from typing import Union, List

from prime_backup.types.units import Duration, Quantity, ByteCount, ValueUnitPair


class UnitTests(unittest.TestCase):
	def test_1_types(self):
		for cls in [Duration, Quantity, ByteCount]:
			for val in [0, '18', 127, 1024, 1440]:
				inst = cls(val + 's' if cls == Duration and isinstance(val, str) else val)  # type: ignore
				self.assertEqual(cls, type(inst))
				self.assertIsInstance(inst, str)
				self.assertEqual(int(val), getattr(inst, 'value'))  # type: ignore

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
			vals: List[Union[int, str]] = [0, 127, 1024, 1440]
			if cls in [Duration]:
				vals.extend(['0s', '18s', '36m'])
			else:
				vals += ['2Gi', '3M', '4ki']
			for val in vals:
				a = cls(val)  # type: ignore
				self.assertEqual(str(a), serializer.serialize(a))

				b = serializer.deserialize(serializer.serialize(a), cls)
				self.assertEqual(a.value, b.value)

				c = json.loads(json.dumps(a))
				self.assertEqual(str(a), c)


if __name__ == '__main__':
	unittest.main()
