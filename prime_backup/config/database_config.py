from mcdreforged.api.utils import Serializable

from prime_backup.types.units import Duration


class CompactDatabaseConfig(Serializable):
	enabled: bool = False
	interval: Duration = Duration('12h')
	jitter: Duration = Duration('1m')


class DatabaseConfig(Serializable):
	compact: CompactDatabaseConfig = CompactDatabaseConfig()
