from mcdreforged.api.utils import Serializable

from prime_backup.types.units import Duration


class ScheduledBackupConfig(Serializable):
	enabled: bool = False
	interval: Duration = Duration('12h')
	jitter: Duration = Duration('10s')
	reset_timer_on_backup: bool = True
