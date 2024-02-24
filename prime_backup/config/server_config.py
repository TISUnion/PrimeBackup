import re
from typing import List

from mcdreforged.api.utils import Serializable

from prime_backup.types.units import Duration


class MinecraftServerCommands(Serializable):
	save_all_worlds: str = 'save-all flush'
	auto_save_off: str = 'save-off'
	auto_save_on: str = 'save-on'


class ServerConfig(Serializable):
	turn_off_auto_save: bool = True
	commands: MinecraftServerCommands = MinecraftServerCommands()
	saved_world_regex: List[re.Pattern] = [
		re.compile('Saved the game'),
		re.compile('Saved the world'),
	]
	save_world_max_wait: Duration = Duration('10min')
