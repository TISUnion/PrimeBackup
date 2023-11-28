import functools
from pathlib import Path
from typing import Optional

from mcdreforged.api.utils import Serializable

from prime_backup.config.sub_configs import CommandConfig, ServerConfig, BackupConfig, RetentionConfig


class Config(Serializable):
	enabled: bool = True
	debug: bool = False
	command: CommandConfig = CommandConfig.get_default()
	server: ServerConfig = ServerConfig.get_default()
	backup: BackupConfig = BackupConfig.get_default()
	retention: RetentionConfig = RetentionConfig.get_default()

	@classmethod
	@functools.lru_cache
	def __get_default(cls) -> 'Config':
		return Config.get_default()

	@classmethod
	def get(cls) -> 'Config':
		if _config is None:
			return cls.__get_default()
		return _config

	@functools.cached_property
	def storage_path(self) -> Path:
		return Path(self.backup.storage_root)

	@functools.cached_property
	def source_path(self) -> Path:
		return Path(self.backup.source_root)


_config: Optional[Config] = None


def set_config_instance(cfg: Config):
	global _config
	_config = cfg
