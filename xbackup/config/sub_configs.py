import functools
import re
from pathlib import Path
from typing import Dict, List, Any

from mcdreforged.api.utils import Serializable

from xbackup.compressors import CompressMethod
from xbackup.config.types import Duration, Quantity, HashMethod


class CommandConfig(Serializable):
	prefix: str = '!!xb'
	permission: Dict[str, int] = {
		# TODO
		'make': 1,
	}


class CustomServerCommands(Serializable):
	save_all_worlds: str = 'save-all flush'
	turn_off_auto_save: str = 'save-off'
	turn_on_auto_save: str = 'save-on'


class ServerConfig(Serializable):
	turn_off_auto_save: bool = True
	commands: CustomServerCommands = CustomServerCommands.get_default()
	saved_world_regex: List[str] = [
		'^Saved the game$',
		'^Saved the world$',
	]
	save_world_max_wait: str = '10min'

	@functools.cached_property
	def saved_world_regex_patterns(self) -> List[re.Pattern]:
		return list(map(re.compile, self.saved_world_regex))

	@functools.cached_property
	def save_world_max_wait_sec(self) -> float:
		return Duration(self.save_world_max_wait).duration

	def validate_attribute(self, attr_name: str, attr_value: Any, **kwargs):
		if attr_name == 'save_world_max_wait':
			Duration(attr_value)
		elif attr_name == 'saved_world_regex':
			try:
				re.compile(attr_value)
			except re.error as e:
				raise ValueError(e)


class BackupConfig(Serializable):
	storage_root: str = './xbackup_files'
	source_root: str = './server'
	targets: List[str] = [
		'world',
	]
	ignores: List[str] = [
		'session.lock',
	]
	hash_method: HashMethod = HashMethod.xxh128
	compress_method: CompressMethod = CompressMethod.plain
	compress_threshold: int = 128
	backup_on_overwrite: bool = True

	def validate_attribute(self, attr_name: str, attr_value: Any, **kwargs):
		if attr_name == 'compress_method':
			if attr_value not in CompressMethod:
				raise ValueError('bad compress method {!r}'.format(attr_value))

	def is_file_ignore(self, full_path: Path) -> bool:
		# TODO
		return False


class RetentionConfig(Serializable):
	size_limit: str = '0'
	amount_limit: int = 100

	def validate_attribute(self, attr_name: str, attr_value: Any, **kwargs):
		if attr_name == 'size_limit':
			Quantity(attr_value)
