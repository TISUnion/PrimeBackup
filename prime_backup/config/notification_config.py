from typing import List, Dict, Optional

from mcdreforged.api.utils import Serializable
from typing_extensions import override

from prime_backup.types.notification_event import NotificationEvent
from prime_backup.types.units import Duration


class BarkOptions(Serializable):
	device_key: Optional[str] = None
	title: Optional[str] = None
	subtitle: Optional[str] = None
	body: Optional[str] = None
	markdown: bool = False
	level: Optional[str] = None
	volume: Optional[int] = None
	badge: Optional[int] = None
	call: Optional[str] = None
	autoCopy: Optional[str] = None
	copy: Optional[str] = None
	sound: Optional[str] = None
	icon: Optional[str] = None
	image: Optional[str] = None
	group: Optional[str] = None
	url: Optional[str] = None
	action: Optional[str] = None
	id: Optional[str] = None
	delete: Optional[str] = None
	isArchive: Optional[str] = None


class NotificationEndpoint(Serializable):
	enabled: bool = True
	name: str = 'default'
	type: str = 'webhook'  # webhook | bark
	url: str = ''
	headers: Dict[str, str] = {}
	timeout: Duration = Duration('5s')
	bark: BarkOptions = BarkOptions()

	@override
	def on_deserialization(self, **kwargs):
		self.type = str(self.type).lower()
		if self.type not in ['webhook', 'bark']:
			raise ValueError('bad notification endpoint type {!r}'.format(self.type))


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
