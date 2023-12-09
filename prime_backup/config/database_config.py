from mcdreforged.api.utils import Serializable

from prime_backup.config.config_common import CrontabJobSetting
from prime_backup.types.units import Duration


class CompactDatabaseConfig(CrontabJobSetting):
	enabled = True
	interval = Duration('1d')
	crontab = None
	jitter = Duration('5m')


class BackUpDatabaseConfig(CrontabJobSetting):
	enabled: bool = True
	interval: Duration = Duration('7d')
	crontab = None
	jitter: Duration = Duration('10m')


class DatabaseConfig(Serializable):
	compact: CompactDatabaseConfig = CompactDatabaseConfig()
	backup: BackUpDatabaseConfig = BackUpDatabaseConfig()
