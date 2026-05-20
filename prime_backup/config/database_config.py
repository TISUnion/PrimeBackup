from mcdreforged.api.utils import Serializable

from prime_backup.config.config_common import CrontabJobSetting
from prime_backup.types.units import Duration


class CompactDatabaseConfig(CrontabJobSetting):
	enabled = True
	interval = None
	crontab = '0 7 * * 0'
	jitter = Duration('1m')


class BackUpDatabaseConfig(CrontabJobSetting):
	enabled = True
	interval = None
	crontab = '0 6 * * 0'
	jitter = Duration('1m')


class CompactPackDatabaseConfig(CrontabJobSetting):
	enabled = True
	interval = None
	crontab = '0 5 * * 0'
	jitter = Duration('1m')

	@property
	def compact_threshold(self) -> float:
		from prime_backup.config.config import Config
		return Config.get().backup.pack_maintenance_compact_threshold


class DatabaseConfig(Serializable):
	compact: CompactDatabaseConfig = CompactDatabaseConfig()
	backup: BackUpDatabaseConfig = BackUpDatabaseConfig()
	compact_pack: CompactPackDatabaseConfig = CompactPackDatabaseConfig()
