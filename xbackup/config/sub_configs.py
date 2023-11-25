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


class BackupJob(Serializable):
	source_root: str = './server'
	targets: List[str] = []
	ignores: List[str] = []
	# TODO: timer

	@functools.cached_property
	def source_path(self) -> Path:
		return Path(self.source_root)

	def is_file_ignore(self, full_path: Path) -> bool:
		# TODO
		return False


class BackupConfig(Serializable):
	storage_root: str = './xbackup_files'
	hash_method: HashMethod = HashMethod.xxh128

	jobs: Dict[str, BackupJob] = {
		'default': BackupJob(
			source_root='./server',
			targets=['world'],
			ignores=['session.lock'],
		),
	}
	compress_method: CompressMethod = CompressMethod.plain
	compress_threshold: int = 128
	backup_on_overwrite: bool = True
	concurrency: int = 1

	def validate_attribute(self, attr_name: str, attr_value: Any, **kwargs):
		if attr_name == 'compress_method':
			if attr_value not in CompressMethod:
				raise ValueError('bad compress method {!r}'.format(attr_value))

	def get_default_job(self) -> BackupJob:
		if len(self.jobs) > 0:
			return next(iter(self.jobs.values()))
		else:
			raise IndexError('no job is defined')


class RetentionConfig(Serializable):
	size_limit: str = '0'
	amount_limit: int = 100

	def validate_attribute(self, attr_name: str, attr_value: Any, **kwargs):
		if attr_name == 'size_limit':
			Quantity(attr_value)
