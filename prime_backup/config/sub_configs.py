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
		'list': 1,
		'make': 1,
		'back': 2,
		'show': 1,
		'del': 2,
		'delete': 2,
		'delete_range': 3,
		'export': 3,
		'rename': 2,
		'confirm': 1,
		'abort': 1,
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
	ignored_files: List[str] = [
		'session.lock',
	]
	hash_method: HashMethod = HashMethod.xxh128
	compress_method: CompressMethod = CompressMethod.zstd
	compress_threshold: int = 64
	backup_on_overwrite: bool = True

	def validate_attribute(self, attr_name: str, attr_value: Any, **kwargs):
		if attr_name == 'compress_method':
			if attr_value not in CompressMethod:
				raise ValueError('bad compress method {!r}'.format(attr_value))

	def get_compress_method_from_size(self, file_size: int) -> CompressMethod:
		if file_size < self.compress_threshold:
			return CompressMethod.plain
		else:
			return self.compress_method

	def is_file_ignore(self, full_path: Path) -> bool:
		"""
		Apply to not only files
		"""
		name = full_path.name
		for item in self.ignored_files:
			if len(item) > 0:
				if item[0] == '*' and name.endswith(item[1:]):
					return True
				if item[-1] == '*' and name.startswith(item[:-1]):
					return True
				if name == item:
					return True
		return False


class RetentionConfig(Serializable):
	"""
	Not used for now
	"""
	size_limit: ByteCount = ByteCount('0B')
	amount_limit: int = 0
