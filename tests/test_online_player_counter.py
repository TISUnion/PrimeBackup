import unittest

from prime_backup.mcdr.online_player_counter import PlayerRecords


class OnlinePlayerCounterTestCase(unittest.TestCase):
	@staticmethod
	def is_player_name_valid(name: str) -> bool:
		return len(name) > 1

	@classmethod
	def create_counter(cls) -> PlayerRecords:
		return PlayerRecords(cls.is_player_name_valid)

	def test_0_join_left(self):
		counter = self.create_counter()

		counter.set_player('foo', True)
		self.assertEqual(1, len(counter.get_records()))
		self.assertEqual(True, counter.create_snapshot().has_valid)
		self.assertEqual(True, counter.create_snapshot().has_valid_online)

		counter.set_player('foo', False)
		self.assertEqual(1, len(counter.get_records()))
		self.assertEqual(True, counter.create_snapshot().has_valid)
		self.assertEqual(False, counter.create_snapshot().has_valid_online)

		counter.set_player('foo', False)
		self.assertEqual(1, len(counter.get_records()))
		self.assertEqual(True, counter.create_snapshot().has_valid)
		self.assertEqual(False, counter.create_snapshot().has_valid_online)

	def test_1_not_exists(self):
		counter = self.create_counter()

		counter.set_player('a', True)
		counter.set_player('a', False, check_exist=True)

		with self.assertRaises(KeyError):
			counter.set_player('bb', False, check_exist=True)
		with self.assertRaises(KeyError):
			counter.set_player('ccc', True, check_exist=True)

		counter.set_player('d', True)
		counter.set_player('d', False)
		counter.set_player('d', True, check_exist=True)
		counter.set_player('ee', False)

	def test_2_valid(self):
		counter = self.create_counter()

		counter.set_player('a', True)
		self.assertEqual(1, len(counter.get_records()))
		self.assertEqual(False, counter.create_snapshot().has_valid)
		self.assertEqual(False, counter.create_snapshot().has_valid_online)

		counter.set_player('a', False)
		self.assertEqual(1, len(counter.get_records()))
		self.assertEqual(False, counter.create_snapshot().has_valid)
		self.assertEqual(False, counter.create_snapshot().has_valid_online)

		counter.set_player('a', True)
		self.assertEqual(1, len(counter.get_records()))
		self.assertEqual(False, counter.create_snapshot().has_valid)
		self.assertEqual(False, counter.create_snapshot().has_valid_online)

		counter.set_player('bb', True)
		self.assertEqual(2, len(counter.get_records()))
		self.assertEqual(True, counter.create_snapshot().has_valid)
		self.assertEqual(True, counter.create_snapshot().has_valid_online)

		counter.set_player('bb', False)
		self.assertEqual(2, len(counter.get_records()))
		self.assertEqual(True, counter.create_snapshot().has_valid)
		self.assertEqual(False, counter.create_snapshot().has_valid_online)

	def test_3_remove_offline(self):
		counter = self.create_counter()

		counter.set_player('a', True)
		counter.set_player('bb', True)
		self.assertEqual(2, len(counter.get_records()))
		self.assertEqual(True, counter.create_snapshot().has_valid)
		self.assertEqual(True, counter.create_snapshot().has_valid_online)

		counter.remove_offline_players()
		self.assertEqual(2, len(counter.get_records()))
		self.assertEqual(True, counter.create_snapshot().has_valid)
		self.assertEqual(True, counter.create_snapshot().has_valid_online)

		counter.set_player('a', False)
		counter.remove_offline_players()
		self.assertEqual(1, len(counter.get_records()))
		self.assertEqual('bb', counter.get_records()[0].name)
		self.assertEqual(True, counter.create_snapshot().has_valid)
		self.assertEqual(True, counter.create_snapshot().has_valid_online)

		counter.set_player('ccc', True)
		counter.remove_offline_players()
		self.assertEqual(2, len(counter.get_records()))
		self.assertEqual(True, counter.create_snapshot().has_valid)
		self.assertEqual(True, counter.create_snapshot().has_valid_online)

		counter.set_player('bb', False)
		counter.remove_offline_players()
		self.assertEqual(1, len(counter.get_records()))
		self.assertEqual('ccc', counter.get_records()[0].name)
		self.assertEqual(True, counter.create_snapshot().has_valid)
		self.assertEqual(True, counter.create_snapshot().has_valid_online)

		counter.set_player('d', True)
		counter.set_player('ccc', False)
		self.assertEqual(2, len(counter.get_records()))
		self.assertEqual(True, counter.create_snapshot().has_valid)
		self.assertEqual(False, counter.create_snapshot().has_valid_online)

		counter.remove_offline_players()
		self.assertEqual(1, len(counter.get_records()))
		self.assertEqual('d', counter.get_records()[0].name)
		self.assertEqual(False, counter.create_snapshot().has_valid)
		self.assertEqual(False, counter.create_snapshot().has_valid_online)


if __name__ == '__main__':
	unittest.main()
