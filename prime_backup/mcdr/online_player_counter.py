import threading
from typing import Any, Optional

from mcdreforged.api.all import *

from prime_backup import logger
from prime_backup.config.config import Config
from prime_backup.utils import misc_utils


class OnlinePlayerCounter:
	__inst: Optional['OnlinePlayerCounter'] = None

	@classmethod
	def get(cls) -> 'OnlinePlayerCounter':
		if cls.__inst is None:
			raise ValueError('not initialized yet')
		return cls.__inst

	def __init__(self):
		if self.__inst is not None:
			raise ValueError('double initialization')
		self.__inst = self
		self.config = Config.get()
		self.logger = logger.get()

		self.lock = threading.Lock()
		self.count_is_correct = False
		self.player_count = 0

	def has_player_online(self) -> Optional[bool]:
		with self.lock:
			if self.count_is_correct:
				return self.player_count > 0
			else:
				return None

	def on_load(self, server: ServerInterface, prev: Any):
		if (prev_lock := getattr(prev, 'lock', None)) is not None and type(prev_lock) is type(self.lock):
			with prev_lock:
				prev_count_is_correct = getattr(prev, 'count_is_correct', False)
				prev_player_count = getattr(prev, 'player_count', None)
		else:
			prev_count_is_correct = False
			prev_player_count = None

		if prev_count_is_correct is True and prev_player_count is not None:
			self.logger.info('Found existing valid data of the previous online player counter, inherit it')
			with self.lock:
				self.count_is_correct = True
				self.player_count = prev_player_count
		elif (api := server.get_plugin_instance('minecraft_data_api')) is not None:
			def query_thread():
				result = api.get_server_player_list(timeout=10)
				if result is not None:
					amount = int(result[0])
					self.logger.info('Successfully query online player amount from minecraft_data_api: {}'.format(amount))
					with self.lock:
						self.count_is_correct = True
						self.player_count = amount
				else:
					self.logger.warning('Query player from minecraft_data_api failed')

			thread = threading.Thread(target=query_thread, name=misc_utils.make_thread_name('query-player-list'), daemon=True)
			thread.start()

	def on_server_start(self):
		self.logger.info('Server startup detected, enable the online player counter')
		with self.lock:
			self.count_is_correct = True
			self.player_count = 0

	def on_player_joined(self):
		with self.lock:
			if self.count_is_correct:
				self.player_count += 1

	def on_player_left(self):
		with self.lock:
			if self.count_is_correct:
				self.player_count = max(0, self.player_count - 1)
