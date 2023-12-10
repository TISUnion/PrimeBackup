from mcdreforged.api.utils import Serializable

from prime_backup.config.config_common import CrontabJobSetting
from prime_backup.types.units import Duration


class CompactDatabaseConfig(CrontabJobSetting):
	enabled = True
	interval = None
	crontab = '0 7 * * *'
	jitter = Duration('1m')


class BackUpDatabaseConfig(CrontabJobSetting):
	enabled = True
	interval = None
	crontab = '0 6 * * 0'
	jitter = Duration('1m')


class DatabaseConfig(Serializable):
	compact: CompactDatabaseConfig = CompactDatabaseConfig()
	backup: BackUpDatabaseConfig = BackUpDatabaseConfig()
