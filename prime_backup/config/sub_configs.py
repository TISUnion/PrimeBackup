import functools
import re
from pathlib import Path
from typing import List, Any, Optional

import pytz
from mcdreforged.api.utils import Serializable

from prime_backup import constants
from prime_backup.compressors import CompressMethod
from prime_backup.types.hash_method import HashMethod
from prime_backup.types.units import Duration


class CommandPermissions(Serializable):
	abort: int = 1
	back: int = 2
	confirm: int = 1
	delete: int = 2
	delete_range: int = 3
	export: int = 3
	list: int = 1
	make: int = 1
	prune: int = 3
	rename: int = 2
	show: int = 1

	def get(self, literal: str) -> int:
		if literal.startswith('_'):
			raise KeyError(literal)
		return getattr(self, literal, constants.DEFAULT_COMMAND_PERMISSION_LEVEL)

	def items(self):
		return self.serialize().items()


class CommandConfig(Serializable):
	prefix: str = '!!pb'
	permission: CommandPermissions = CommandPermissions()
	confirm_time_wait: Duration = Duration('60s')
	backup_on_restore: bool = True
	restore_countdown_sec: int = 10


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


class ScheduledBackupConfig(Serializable):
	enabled: bool = False
	interval: Duration = Duration('12h')
	jitter: Duration = Duration('10s')
	reset_timer_on_backup: bool = True


class BackupConfig(Serializable):
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

	def get_compress_method_from_size(self, file_size: int) -> CompressMethod:
		if file_size < self.compress_threshold:
			return CompressMethod.plain
		else:
			return self.compress_method

	def is_file_ignore(self, full_path: Path) -> bool:
		"""
		Apply to not only files
		"""
		# TODO: better rule?
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


class PruneSetting(Serializable):
	enabled: bool = False

	max_amount: int = 0
	max_lifetime: Duration = Duration('0s')

	# https://pve.proxmox.com/wiki/Backup_and_Restore#vzdump_retention
	# https://pbs.proxmox.com/docs/prune-simulator/
	# -1 means infinity
	last: int = -1
	hour: int = 0
	day: int = 0
	week: int = 0
	month: int = 0
	year: int = 0


class PruneConfig(Serializable):
	interval: Duration = Duration('3h')
	jitter: Duration = Duration('10s')
	timezone_override: Optional[str] = None
	regular_backup: PruneSetting = PruneSetting()
	pre_restore_backup: PruneSetting = PruneSetting(
		enabled=True,
		max_amount=10,
		max_lifetime=Duration('30d'),
	)

	def validate_attribute(self, attr_name: str, attr_value: Any, **kwargs):
		if attr_name == 'timezone_override' and attr_value is not None:
			try:
				pytz.timezone(attr_value)
			except pytz.UnknownTimeZoneError as e:
				raise ValueError('bad timezone {!r}: {}'.format(attr_value, e))
