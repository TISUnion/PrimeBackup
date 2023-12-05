import functools
import re
from typing import List, Any

from mcdreforged.api.utils import Serializable

from prime_backup.types.units import Duration


class MinecraftServerCommands(Serializable):
	save_all_worlds: str = 'save-all flush'
	auto_save_off: str = 'save-off'
	auto_save_on: str = 'save-on'


class ServerConfig(Serializable):
	turn_off_auto_save: bool = True
	commands: MinecraftServerCommands = MinecraftServerCommands()
	saved_world_regex: List[str] = [
		'^Saved the game$',
		'^Saved the world$',
	]
	save_world_max_wait: Duration = Duration('10min')

	@functools.cached_property
	def saved_world_regex_patterns(self) -> List[re.Pattern]:
		return list(map(re.compile, self.saved_world_regex))

	def validate_attribute(self, attr_name: str, attr_value: Any, **kwargs):
		if attr_name == 'saved_world_regex':
			try:
				for regex in attr_value:
					re.compile(regex)
			except re.error as e:
				raise ValueError(e)
