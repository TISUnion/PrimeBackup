from typing import Optional, Any

import pytz
from mcdreforged.api.utils import Serializable

from prime_backup.types.units import Duration


class PruneSetting(Serializable):
	enabled: bool = False

	# <=0 means no limit
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
