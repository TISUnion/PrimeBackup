import functools
from pathlib import Path
from typing import Optional

from mcdreforged.api.utils import Serializable

from xbackup.config.sub_configs import CommandConfig, ServerConfig, BackupConfig, RetentionConfig


class Config(Serializable):
	enabled: bool = False
	command: CommandConfig = CommandConfig.get_default()
	server: ServerConfig = ServerConfig.get_default()
	backup: BackupConfig = BackupConfig.get_default()
	retention: RetentionConfig = RetentionConfig.get_default()

	@classmethod
	def get(cls) -> 'Config':
		if _config is None:
			return Config.get_default()
		return _config

	@functools.cached_property
	def storage_path(self):
		return Path(self.backup.storage_root)

	@functools.cached_property
	def source_path(self):
		return Path(self.backup.source_root)


_config: Optional[Config] = None


def set_config_instance(cfg: Config):
	global _config
	_config = cfg
