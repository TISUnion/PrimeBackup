import dataclasses
import functools
import threading
from typing import Any, Optional, List, Dict, Callable

from mcdreforged.api.all import ServerInterface
from typing_extensions import Final

from prime_backup import logger
from prime_backup.config.config import Config
from prime_backup.utils import misc_utils


@dataclasses.dataclass(frozen=True)
class PlayerRecord:
	name: str
	online: bool
	valid: bool


class PlayerRecords:
	__NameValidator = Callable[[str], bool]

	def __init__(self, name_validator: __NameValidator):
		self.__name_validator = functools.lru_cache(maxsize=256)(name_validator)
		self.__players: Dict[str, PlayerRecord] = {}

	def get_records(self) -> List[PlayerRecord]:
		return list(self.__players.values())

	def remove_offline_players(self):
		for player, record in list(self.__players.items()):
			if not record.online:
				self.__players.pop(player)

	def set_player(self, player: str, online: bool, *, check_exist: bool = False):
		if check_exist:
			_ = self.__players[player]
		self.__players[player] = PlayerRecord(
			name=player,
			online=online,
			valid=self.__name_validator(player),
		)

	def clear(self):
		self.__players.clear()

	@dataclasses.dataclass(frozen=True)
	class Snapshot:
		summary: str
		has_valid: bool
		has_valid_online: bool

	def create_snapshot(self) -> Snapshot:
		online_prefix = '', '*'
		return self.Snapshot(
			summary='valid {}, ignored {}'.format(
				[
					online_prefix[record.online] + record.name
					for record in self.__players.values()
					if record.valid
				],
				[
					online_prefix[record.online] + record.name
					for record in self.__players.values()
					if not record.valid
				],
			),
			has_valid=any(record.valid for record in self.__players.values()),
			has_valid_online=any(record.valid and record.online for record in self.__players.values()),
		)


class OnlinePlayerCounter:
	__inst: Optional['OnlinePlayerCounter'] = None

	@classmethod
	def get(cls) -> 'OnlinePlayerCounter':
		if cls.__inst is None:
			raise ValueError('not initialized yet')
		return cls.__inst

	def __init__(self, server: ServerInterface):
		cls = type(self)
		if cls.__inst is not None:
			raise ValueError('double initialization')
		cls.__inst = self
		self.config = Config.get()
		self.logger = logger.get()
		self.server = server

		self.data_lock = threading.Lock()
		self.data_is_correct = False
		self.player_records: Final[PlayerRecords] = PlayerRecords(self.__is_valid_player)

	def __is_valid_player(self, player_name: str) -> bool:
		blacklist = self.config.scheduled_backup.require_online_players_blacklist
		return all(
			not pattern.fullmatch(player_name)
			for pattern in blacklist
		)

	def get_player_record_snapshot(self) -> Optional[PlayerRecords.Snapshot]:
		with self.data_lock:
			if self.data_is_correct:
				return self.player_records.create_snapshot()
			else:
				return None

	def remove_offline_player_records(self):
		with self.data_lock:
			if self.data_is_correct:
				self.player_records.remove_offline_players()

	def __try_update_player_records_from_api(self, *, log_success: bool = False, timeout: float = 10):
		api = self.server.get_plugin_instance('minecraft_data_api')
		if api is None:
			return

		def query_thread():
			player_names: List[str] = []
			try:
				result = api.get_server_player_list(timeout=timeout)
				if result is not None:
					player_names = list(map(str, result[2]))
			except Exception as e:
				self.logger.exception('Queried players from minecraft_data_api error', e)
				return
			if result is None:
				self.logger.warning('Queried players from minecraft_data_api failed')
				return

			with self.data_lock:
				self.data_is_correct = True
				self.player_records.clear()
				for name in player_names:
					self.player_records.set_player(name, True)

			if log_success:
				self.logger.info('Successfully queried online players from minecraft_data_api: {}'.format(player_names))

		thread = threading.Thread(target=query_thread, name=misc_utils.make_thread_name('player-query'), daemon=True)
		thread.start()

	def on_load(self, prev: Any):
		should_update_from_api = self.server.is_server_startup()

		# delay this for a little bit, in case minecraft_data_api is loading too
		self.server.schedule_task(functools.partial(self.__on_load, prev, should_update_from_api))

	def __on_load(self, prev: Any, should_update_from_api: bool):
		if type(prev_lock := getattr(prev, 'data_lock', None)) is type(self.data_lock):
			with prev_lock:
				prev_data_is_correct: bool = getattr(prev, 'data_is_correct', False)
				prev_player_records: Optional[PlayerRecords] = getattr(prev, 'player_records', None)
				if prev_data_is_correct and prev_player_records is not None:
					self.logger.debug('Found existing valid data of the previous online player counter')
					with self.data_lock:
						self.data_is_correct = True
						self.player_records.clear()
						try:
							for record in prev_player_records.get_records():
								self.player_records.set_player(record.name, record.online)
						except AttributeError as e:
							self.data_is_correct = False
							self.player_records.clear()
							self.logger.warning('Inherited data from previous online player counter failed: {}'.format(e))
						else:
							self.logger.info('Inherited data from previous online player counter: {}'.format(self.player_records.create_snapshot().summary))

		if not self.data_is_correct and should_update_from_api and self.server.is_server_startup():
			self.__try_update_player_records_from_api(log_success=True)

	def on_server_start_stop(self, what: str):
		self.logger.debug(f'Server {what} detected, enable the online player counter')
		with self.data_lock:
			self.data_is_correct = True
			self.player_records.clear()

	def on_player_joined(self, player: str):
		with self.data_lock:
			if self.data_is_correct:
				self.player_records.set_player(player, True)

	def on_player_left(self, player: str):
		with self.data_lock:
			if self.data_is_correct:
				try:
					self.player_records.set_player(player, False, check_exist=True)
				except KeyError:
					self.logger.warning('Tried to mark non-existent player {} as offline from player dict {}, data desync?'.format(player, self.player_records))
