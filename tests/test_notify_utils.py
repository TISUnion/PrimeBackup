import json
import unittest
from unittest.mock import patch

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


if __name__ == '__main__':
	unittest.main()
