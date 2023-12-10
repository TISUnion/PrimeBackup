from typing import Optional, Any

from apscheduler.triggers.cron import CronTrigger
from mcdreforged.api.utils import Serializable

from prime_backup.types.units import Duration


def _validate_crontab_str(value: Optional[str]):
	if value is not None:
		try:
			CronTrigger.from_crontab(value)
		except Exception as e:
			raise ValueError('bad crontab string {!r}: {}'.format(value, e))


class CrontabJobSetting(Serializable):
	enabled: bool
	interval: Optional[Duration]
	crontab: Optional[str]
	jitter: Duration

	def validate_attribute(self, attr_name: str, attr_value: Any, **kwargs):
		if attr_name == 'crontab':
			_validate_crontab_str(attr_value)

	def on_deserialization(self, **kwargs):
		if self.enabled:
			if self.interval is None and self.crontab is None:
				raise ValueError('Field interval and crontab cannot be None at the same time')
			if self.interval is not None and self.crontab is not None:
				raise ValueError('Field interval and crontab cannot be set at the same time')
