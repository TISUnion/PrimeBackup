import re
from typing import Optional, List

from prime_backup.config.config_common import CrontabJobSetting
from prime_backup.types.units import Duration


class ScheduledBackupConfig(CrontabJobSetting):
	enabled: bool = False
	interval: Optional[Duration] = Duration('12h')
	crontab: Optional[str] = None
	jitter: Duration = Duration('10s')

	reset_timer_on_backup: bool = True
	require_online_players: bool = False
	require_online_players_blacklist: List[re.Pattern] = []

	def on_deserialization(self, **kwargs):
		# recompile patterns with re.IGNORECASE
		self.require_online_players_blacklist = [
			re.compile(p.pattern, re.IGNORECASE)
			for p in self.require_online_players_blacklist
		]
