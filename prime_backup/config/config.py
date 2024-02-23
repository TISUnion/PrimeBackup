import functools
from pathlib import Path
from typing import Optional

from mcdreforged.api.all import Serializable

from prime_backup.config.backup_config import BackupConfig
from prime_backup.config.command_config import CommandConfig
from prime_backup.config.database_config import DatabaseConfig
from prime_backup.config.prune_config import PruneConfig
from prime_backup.config.scheduled_backup_config import ScheduledBackupConfig
from prime_backup.config.server_config import ServerConfig


class Config(Serializable):
	enabled: bool = False
	debug: bool = False
	storage_root: str = './pb_files'
	concurrency: int = 1

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

	def get_effective_concurrency(self) -> int:
		if self.concurrency == 0:
			import multiprocessing
			return max(1, int(multiprocessing.cpu_count() * 0.5))
		else:
			return max(1, self.concurrency)

	@property
	def storage_path(self) -> Path:
		return Path(self.storage_root)

	@property
	def blobs_path(self) -> Path:
		return self.storage_path / 'blobs'

	@property
	def temp_path(self) -> Path:
		return self.storage_path / 'temp'

	@property
	def source_path(self) -> Path:
		if self.backup.source_root_use_mcdr_working_directory:
			from mcdreforged.api.all import ServerInterface
			si = ServerInterface.si()
			if si is not None and (mcdr_wd := si.get_mcdr_config().get('working_directory')) is not None:
				return Path(mcdr_wd)
		return Path(self.backup.source_root)


_config: Optional[Config] = None


def set_config_instance(cfg: Config):
	global _config
	_config = cfg
