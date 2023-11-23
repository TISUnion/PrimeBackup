import functools
import re
from pathlib import Path
from typing import Dict, List, Any

from mcdreforged.api.utils import Serializable

from xbackup.compressors import CompressMethod
from xbackup.config.types import Duration, Quantity


class CommandConfig(Serializable):
	prefix: str = '!!xb'
	permission: Dict[str, int] = {
		# TODO
		'make': 1,
	}


class ServerConfig(Serializable):
	turn_off_autosave: bool = True
	saved_world_regex: List[str] = [
		'^Saved the game$',
		'^Saved the world$',
	]
	save_world_max_wait: str = '10min'

	@functools.cached_property
	def saved_world_regex_patterns(self) -> List[re.Pattern]:
		return list(map(re.compile, self.saved_world_regex))

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
	compress_method: CompressMethod = CompressMethod.zstd
	compress_threshold: int = 256
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
