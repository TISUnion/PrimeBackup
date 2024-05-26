import dataclasses
import functools
import threading
from typing import Any, Optional, List

from mcdreforged.api.all import *

from prime_backup import logger
from prime_backup.config.config import Config
from prime_backup.utils import misc_utils

_PlayerList = List[str]


@dataclasses.dataclass(frozen=True)
class GetOnlinePlayersResult:
	all: _PlayerList = dataclasses.field(default_factory=list)
	valid: _PlayerList = dataclasses.field(default_factory=list)
	ignored: _PlayerList = dataclasses.field(default_factory=list)


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
		self.player_list: _PlayerList = []

	def get_online_players(self) -> Optional[GetOnlinePlayersResult]:
		def filter_for(players: _PlayerList):
			blacklist = self.config.scheduled_backup.require_online_players_blacklist
			result = GetOnlinePlayersResult()
			for player in players:
				ok = all(map(
					lambda pattern: not pattern.fullmatch(player),
					blacklist
				))
				result.all.append(player)
				if ok:
					result.valid.append(player)
				else:
					result.ignored.append(player)

			return result

		with self.data_lock:
			if self.data_is_correct:
				return filter_for(self.player_list)
			else:
				return None

	def __try_update_player_list_from_api(self, *, log_success: bool = False, timeout: float = 10):
		api = self.server.get_plugin_instance('minecraft_data_api')
		if api is None:
			return None

		def query_thread():
			player_names: _PlayerList = []
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
				self.player_list = player_names

			if log_success:
				self.logger.info('Successfully queried online players from minecraft_data_api: {}'.format(player_names))

		thread = threading.Thread(target=query_thread, name=misc_utils.make_thread_name('player-query'), daemon=True)
		thread.start()

	def on_load(self, prev: Any):
		should_update_from_api = self.server.is_server_running()

		# delay this for a little bit, in case minecraft_data_api is loading too
		self.server.schedule_task(functools.partial(self.__on_load, prev, should_update_from_api))

	def __on_load(self, prev: Any, should_update_from_api: bool):
		if (prev_lock := getattr(prev, 'data_lock', None)) is not None and type(prev_lock) is type(self.data_lock):
			with prev_lock:
				prev_data_is_correct = getattr(prev, 'data_is_correct', False)
				prev_player_list = getattr(prev, 'player_list', None)
				if isinstance(prev_player_list, list):
					prev_player_list = prev_player_list.copy()

				self.job_data_store = getattr(prev, 'job_data_store', {})
		else:
			prev_data_is_correct = False
			prev_player_list = None

		if prev_data_is_correct is True and prev_player_list is not None:
			self.logger.info('Found existing valid data of the previous online player counter, inherit it')
			with self.data_lock:
				self.data_is_correct = True
				self.player_list = prev_player_list
		elif should_update_from_api:
			self.__try_update_player_list_from_api(log_success=True)

	def on_server_start(self):
		self.logger.info('Server startup detected, enable the online player counter')
		with self.data_lock:
			self.data_is_correct = True
			self.player_list = []

	def on_player_joined(self, player: str):
		with self.data_lock:
			if self.data_is_correct:
				if player not in self.player_list:
					self.player_list.append(player)

	def on_player_left(self, player: str):
		with self.data_lock:
			if self.data_is_correct:
				try:
					self.player_list.remove(player)
				except ValueError:
					self.logger.warning('Tried to remove not-existed player {} from player list {}, data desync?'.format(player, self.player_list))
