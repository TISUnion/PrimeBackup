import functools
import re
from pathlib import Path
from typing import Dict, List, Any

from mcdreforged.api.utils import Serializable

from prime_backup.compressors import CompressMethod
from prime_backup.types.hash_method import HashMethod
from prime_backup.types.units import Duration, ByteCount


class CommandConfig(Serializable):
	prefix: str = '!!pb'
	permission: Dict[str, int] = {
		# TODO
		'make': 1,
	}
	confirm_time_wait: Duration = Duration('60s')


class CustomServerCommands(Serializable):
	save_all_worlds: str = 'save-all flush'
	auto_save_off: str = 'save-off'
	auto_save_on: str = 'save-on'


class ServerConfig(Serializable):
	turn_off_auto_save: bool = True
	commands: CustomServerCommands = CustomServerCommands.get_default()
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


class BackupConfig(Serializable):
	storage_root: str = './pb_files'
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
		# TODO: proper impl
		return full_path.name in self.ignores


class RetentionConfig(Serializable):
	size_limit: ByteCount = ByteCount('0B')
	amount_limit: int = 100
