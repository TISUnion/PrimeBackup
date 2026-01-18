import json
import unittest
from unittest.mock import patch

from prime_backup.config.notification_config import NotificationEndpoint, BarkOptions
from prime_backup.types.notification_event import NotificationEvent
from prime_backup.utils import notify_utils


class _DummyVersion:
	def __str__(self) -> str:
		return '1.2.3'


class NotifyUtilsTestCase(unittest.TestCase):
	def test_make_payload_version_json_serializable(self):
		with patch('prime_backup.utils.notify_utils._get_plugin_version', return_value=_DummyVersion()):
			payload = notify_utils._make_payload(
				NotificationEvent.backup_start,
				backup=None,
				operator=None,
				source=None,
				cost_s=None,
				message=None,
				error=None,
				extra=None,
			)
			self.assertEqual('1.2.3', payload['plugin']['version'])
			json.dumps(payload, ensure_ascii=False)

	def test_bark_payload_support(self):
		endpoint = NotificationEndpoint(
			type='bark',
			url='https://api.day.app/push',
			bark=BarkOptions(device_key='abc', group='pb', markdown=True),
		)
		base_payload = {
			'title': 'PrimeBackup backup success',
			'body': 'event=backup_success, backup=#1',
			'event': 'backup_success',
			'status': 'success',
			'task': 'backup',
			'backup': {
				'id': 1,
				'file_count': 10,
				'raw_size': 100,
				'stored_size': 80,
			},
		}
		bark_payload = notify_utils._make_bark_payload(base_payload, endpoint)
		self.assertIn('device_key', bark_payload)
		self.assertIn('markdown', bark_payload)
		self.assertNotIn('body', bark_payload)
		self.assertEqual('pb', bark_payload['group'])
		self.assertEqual('passive', bark_payload['level'])
		self.assertIn('backup', bark_payload['markdown'])

	def test_bark_url_placeholder(self):
		endpoint = NotificationEndpoint(
			type='bark',
			url='https://api.day.app/{device_key}',
			bark=BarkOptions(device_key='xyz'),
		)
		url = notify_utils._resolve_bark_url(endpoint)
		self.assertEqual('https://api.day.app/xyz', url)

	def test_bark_level_default_failure(self):
		endpoint = NotificationEndpoint(type='bark', url='https://api.day.app/{device_key}', bark=BarkOptions(device_key='xyz'))
		base_payload = {
			'title': 'PrimeBackup backup failure',
			'event': 'backup_failure',
			'status': 'failure',
			'task': 'backup',
			'error': {
				'type': 'RuntimeError',
				'message': 'boom',
			},
		}
		bark_payload = notify_utils._make_bark_payload(base_payload, endpoint)
		self.assertEqual('critical', bark_payload['level'])


if __name__ == '__main__':
	unittest.main()
