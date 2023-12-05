import functools
from pathlib import Path
from typing import Optional

from mcdreforged.api.all import Serializable

from prime_backup.config.backup_config import BackupConfig
from prime_backup.config.command_config import CommandConfig
from prime_backup.config.database_config import DatabaseConfig
from prime_backup.config.prune_config import PruneConfig
from prime_backup.config.scheduled_backup import ScheduledBackupConfig
from prime_backup.config.server_config import ServerConfig


class Config(Serializable):
	enabled: bool = True
	debug: bool = False
	storage_root: str = './pb_files'

	command: CommandConfig = CommandConfig()
	server: ServerConfig = ServerConfig()
	backup: BackupConfig = BackupConfig()
	scheduled_backup: ScheduledBackupConfig = ScheduledBackupConfig()
	prune: PruneConfig = PruneConfig()
	database: DatabaseConfig = DatabaseConfig()

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
		return Path(self.storage_root)

	@functools.cached_property
	def source_path(self) -> Path:
		return Path(self.backup.source_root)


_config: Optional[Config] = None


def set_config_instance(cfg: Config):
	global _config
	_config = cfg
