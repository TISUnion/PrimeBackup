from typing import List, Dict

from mcdreforged.api.utils import Serializable
from typing_extensions import override

from prime_backup.types.notification_event import NotificationEvent
from prime_backup.types.units import Duration


class NotificationEndpoint(Serializable):
	enabled: bool = True
	name: str = 'default'
	url: str = ''
	headers: Dict[str, str] = {}
	timeout: Duration = Duration('5s')


class NotificationConfig(Serializable):
	enabled: bool = False
	events: List[NotificationEvent] = [
		NotificationEvent.backup_start,
		NotificationEvent.backup_success,
		NotificationEvent.backup_failure,
		NotificationEvent.restore_start,
		NotificationEvent.restore_success,
		NotificationEvent.restore_failure,
	]
	endpoints: List[NotificationEndpoint] = []

	@override
	def on_deserialization(self, **kwargs):
		normalized_events: List[NotificationEvent] = []
		for item in self.events:
			if isinstance(item, NotificationEvent):
				normalized_events.append(item)
			elif isinstance(item, str):
				if item in NotificationEvent.__members__:
					normalized_events.append(NotificationEvent[item])
				else:
					normalized_events.append(NotificationEvent(item))
			else:
				raise ValueError('bad notification event {!r}'.format(item))
		self.events = normalized_events
