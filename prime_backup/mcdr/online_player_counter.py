import dataclasses
import functools
import threading
from typing import Any, Optional, List, Dict

from mcdreforged.api.all import ServerInterface

from prime_backup import logger
from prime_backup.config.config import Config
from prime_backup.utils import misc_utils


@dataclasses.dataclass(frozen=True)
class PlayerRecord:
	name: str
	online: bool
	valid: bool


_PlayerDict = Dict[str, PlayerRecord]


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
		self.job_data_store = {}
		self.player_dict: _PlayerDict = {}

	def __is_valid_player(self, player_name: str) -> bool:
		blacklist = self.config.scheduled_backup.require_online_players_blacklist
		for pattern in blacklist:
			if not pattern.fullmatch(player_name):
				return False

		return True

	def get_player_records(self) -> Optional[_PlayerDict]:
		with self.data_lock:
			if self.data_is_correct:
				# deepcopy is not needed here, since PlayerRecord is immutable
				return self.player_dict.copy()
			else:
				return None

	def reset_player_records(self):
		with self.data_lock:
			if self.data_is_correct:
				self.player_dict = {
					name: player_record
					for name, player_record in self.player_dict.items()
					if player_record.online
				}

	def __try_update_player_dict_from_api(self, *, log_success: bool = False, timeout: float = 10):
		api = self.server.get_plugin_instance('minecraft_data_api')
		if api is None:
			return None

		def query_thread():
			player_names: List[str] = []
			try:
				result = api.get_server_player_list(timeout=timeout)
				if result is not None:
					player_names = list(map(str, result[2]))
			except Exception as e:
				self.logger.exception('Queried players from minecraft_data_api error', e)
				return None
			if result is None:
				self.logger.warning('Queried players from minecraft_data_api failed')
				return None

			with self.data_lock:
				self.data_is_correct = True
				self.player_dict = {
					name: PlayerRecord(
						name=name,
						online=True,
						valid=self.__is_valid_player(name),
					)
					for name in player_names
				}

			if log_success:
				self.logger.info('Successfully queried online players from minecraft_data_api: {}'.format(player_names))

		thread = threading.Thread(target=query_thread, name=misc_utils.make_thread_name('player-query'), daemon=True)
		thread.start()

	def on_load(self, prev: Any):
		should_update_from_api = self.server.is_server_startup()

		# delay this for a little bit, in case minecraft_data_api is loading too
		self.server.schedule_task(functools.partial(self.__on_load, prev, should_update_from_api))

	def __on_load(self, prev: Any, should_update_from_api: bool):
		if (prev_lock := getattr(prev, 'data_lock', None)) is not None and type(prev_lock) is type(self.data_lock):
			with prev_lock:
				prev_data_is_correct = getattr(prev, 'data_is_correct', False)
				prev_player_dict: Optional[_PlayerDict] = getattr(prev, 'player_dict', None)
				if not isinstance(prev_player_dict, dict) or not all(
						isinstance(record, PlayerRecord)
						for record in prev_player_dict.values()
				):
					prev_data_is_correct = False
					prev_player_dict = None

				self.job_data_store = getattr(prev, 'job_data_store', {})
		else:
			prev_data_is_correct = False
			prev_player_dict = None

		if prev_data_is_correct is True and prev_player_dict is not None:
			self.logger.info('Found existing valid data of the previous online player counter, inherit it')
			with self.data_lock:
				self.data_is_correct = True
				self.player_dict = {
					name: PlayerRecord(
						name=player.name,
						online=player.online,
						# Re-validate player name
						valid=self.__is_valid_player(player.name),
					)
					for name, player in prev_player_dict.items()
				}
		elif should_update_from_api and self.server.is_server_startup():
			self.__try_update_player_dict_from_api(log_success=True)

	def on_server_start_stop(self, what: str):
		self.logger.debug(f'Server {what} detected, enable the online player counter')
		with self.data_lock:
			self.data_is_correct = True
			self.player_dict = {}

	def on_player_joined(self, player_name: str):
		with self.data_lock:
			if self.data_is_correct:
				if player_name not in self.player_dict:
					self.player_dict[player_name] = PlayerRecord(
						name=player_name,
						online=True,
						valid=self.__is_valid_player(player_name),
					)
				else:
					self.player_dict[player_name] = dataclasses.replace(
						self.player_dict[player_name], online=True
					)

	def on_player_left(self, player: str):
		with self.data_lock:
			if self.data_is_correct:
				if player in self.player_dict:
					self.player_dict[player] = dataclasses.replace(
						self.player_dict[player], online=False
					)
				else:
					self.logger.warning('Tried to mark non-existent player {} as offline from player dict {}, data desync?'.format(player, self.player_dict))
