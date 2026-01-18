import datetime
import json
import time
import urllib.request
from typing import Optional, Any, Dict

from prime_backup import constants
from prime_backup import logger
from prime_backup.config.config import Config
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.notification_event import NotificationEvent
from prime_backup.types.operator import Operator


def _get_plugin_version() -> str:
	try:
		from prime_backup.mcdr import mcdr_globals
		return mcdr_globals.metadata.version
	except Exception:
		try:
			from prime_backup.cli import cli_utils
			return cli_utils.get_plugin_version()
		except Exception:
			return '?'


def _get_server_running() -> Optional[bool]:
	try:
		from prime_backup.mcdr import mcdr_globals
		return mcdr_globals.server.is_server_running()
	except Exception:
		return None


def _get_source_info(source: Optional[Any]) -> Optional[Dict[str, Any]]:
	if source is None:
		return None
	try:
		if getattr(source, 'is_player', False):
			return {
				'type': 'player',
				'name': source.player,
			}
		if getattr(source, 'is_console', False):
			return {
				'type': 'console',
				'name': '',
			}
		return {
			'type': 'command_source',
			'name': str(source),
		}
	except Exception:
		return {
			'type': 'unknown',
			'name': str(source),
		}


def _backup_to_payload(backup: BackupInfo) -> Dict[str, Any]:
	return {
		'id': backup.id,
		'date': backup.date_str,
		'comment': backup.comment,
		'creator': str(backup.creator),
		'targets': backup.targets,
		'tags': backup.tags.to_dict(),
		'file_count': backup.file_count,
		'raw_size': backup.raw_size,
		'stored_size': backup.stored_size,
	}


def _operator_to_payload(operator: Operator) -> Dict[str, Any]:
	return {
		'type': operator.type,
		'name': operator.name,
		'full': str(operator),
	}


def _make_payload(
		event: NotificationEvent, *,
		backup: Optional[BackupInfo],
		operator: Optional[Operator],
		source: Optional[Any],
		cost_s: Optional[float],
		message: Optional[str],
		error: Optional[Exception],
		extra: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
	now = time.time()
	version = str(_get_plugin_version())
	payload: Dict[str, Any] = {
		'event': event.value,
		'task': event.task,
		'status': event.status,
		'timestamp': {
			'unix': now,
			'iso': datetime.datetime.utcfromtimestamp(now).isoformat() + 'Z',
		},
		'plugin': {
			'id': constants.PLUGIN_ID,
			'version': version,
		},
		'server': {
			'running': _get_server_running(),
		},
	}

	if backup is not None:
		payload['backup'] = _backup_to_payload(backup)
	if operator is not None:
		payload['operator'] = _operator_to_payload(operator)
	if (src := _get_source_info(source)) is not None:
		payload['source'] = src
	if cost_s is not None:
		payload['cost_s'] = round(cost_s, 3)
	if message is not None:
		payload['message'] = message
	if error is not None:
		payload['error'] = {
			'type': error.__class__.__name__,
			'message': str(error),
		}
	if extra is not None:
		payload['extra'] = extra

	title = f'PrimeBackup {event.task} {event.status}'
	body_parts = [f'event={event.value}']
	if backup is not None:
		body_parts.append(f'backup=#{backup.id}')
	if message:
		body_parts.append(f'message={message}')
	payload['title'] = title
	payload['body'] = ', '.join(body_parts)
	payload['desp'] = payload['body']
	return payload


def _post_json(url: str, payload: Dict[str, Any], headers: Dict[str, str], timeout_s: float):
	data = json.dumps(payload, ensure_ascii=False).encode('utf8')
	request_headers = {
		'Content-Type': 'application/json',
		'User-Agent': f'{constants.PLUGIN_ID}/{_get_plugin_version()}',
	}
	request_headers.update(headers)
	request = urllib.request.Request(url, data=data, headers=request_headers, method='POST')
	with urllib.request.urlopen(request, timeout=timeout_s) as response:
		response.read()


def _apply_if_not_none(data: Dict[str, Any], key: str, value: Any):
	if value is not None:
		data[key] = value


def _format_bark_body(base_payload: Dict[str, Any], *, markdown: bool) -> str:
	backup = base_payload.get('backup') or {}
	operator = base_payload.get('operator') or {}
	source = base_payload.get('source') or {}
	error = base_payload.get('error') or {}

	def format_source() -> Optional[str]:
		src_type = source.get('type')
		if not src_type:
			return None
		src_name = source.get('name') or ''
		return f'{src_type}:{src_name}' if len(src_name) > 0 else src_type

	fields = [
		('Event', base_payload.get('event')),
		('Task', base_payload.get('task')),
		('Status', base_payload.get('status')),
		('Backup', f"#{backup.get('id')}" if backup.get('id') is not None else None),
		('Date', backup.get('date')),
		('Comment', backup.get('comment')),
		('Creator', backup.get('creator')),
		('Operator', operator.get('full')),
		('Source', format_source()),
		('Files', (
			f"{backup.get('file_count')} files, raw={backup.get('raw_size')}, stored={backup.get('stored_size')}"
			if backup.get('file_count') is not None else None
		)),
		('Cost', f"{base_payload.get('cost_s')}s" if base_payload.get('cost_s') is not None else None),
		('Message', base_payload.get('message')),
		('Error', (
			f"{error.get('type')}: {error.get('message')}"
			if error.get('type') or error.get('message') else None
		)),
	]

	if markdown:
		lines = [f"- **{k}**: {v}" for k, v in fields if v not in [None, '']]
		return '\n'.join(lines)
	else:
		lines = [f"{k}: {v}" for k, v in fields if v not in [None, '']]
		return '\n'.join(lines)


def _resolve_bark_url(endpoint) -> str:
	url = endpoint.url
	device_key = getattr(endpoint.bark, 'device_key', None)
	if device_key:
		url = url.replace('{device_key}', device_key).replace('{key}', device_key)
	return url


def _make_bark_payload(base_payload: Dict[str, Any], endpoint) -> Dict[str, Any]:
	bark = endpoint.bark
	markdown = bool(bark.markdown)
	default_body = _format_bark_body(base_payload, markdown=markdown)
	title = bark.title or base_payload.get('title')
	body = bark.body or default_body

	backup = base_payload.get('backup') or {}
	backup_id = backup.get('id')
	if title and backup_id is not None and f'#{backup_id}' not in title:
		title = f'{title} #{backup_id}'

	data: Dict[str, Any] = {}
	if markdown:
		data['markdown'] = body
	else:
		data['body'] = body
	_apply_if_not_none(data, 'title', title)
	_apply_if_not_none(data, 'subtitle', bark.subtitle)

	if bark.level is None:
		status = base_payload.get('status')
		if status == 'failure':
			data['level'] = 'critical'
		elif status == 'success':
			data['level'] = 'passive'
		else:
			data['level'] = 'active'
	else:
		data['level'] = bark.level
	_apply_if_not_none(data, 'volume', bark.volume)
	_apply_if_not_none(data, 'badge', bark.badge)
	_apply_if_not_none(data, 'call', bark.call)
	_apply_if_not_none(data, 'autoCopy', bark.autoCopy)
	_apply_if_not_none(data, 'copy', bark.copy)
	_apply_if_not_none(data, 'sound', bark.sound)
	_apply_if_not_none(data, 'icon', bark.icon)
	_apply_if_not_none(data, 'image', bark.image)
	_apply_if_not_none(data, 'group', bark.group or constants.PLUGIN_ID)
	_apply_if_not_none(data, 'url', bark.url)
	_apply_if_not_none(data, 'action', bark.action)
	_apply_if_not_none(data, 'id', bark.id)
	_apply_if_not_none(data, 'delete', bark.delete)
	_apply_if_not_none(data, 'isArchive', bark.isArchive)

	if bark.device_key and _resolve_bark_url(endpoint).rstrip('/').endswith('/push'):
		data['device_key'] = bark.device_key

	return data


def notify(
		event: NotificationEvent, *,
		backup: Optional[BackupInfo] = None,
		operator: Optional[Operator] = None,
		source: Optional[Any] = None,
		cost_s: Optional[float] = None,
		message: Optional[str] = None,
		error: Optional[Exception] = None,
		extra: Optional[Dict[str, Any]] = None,
):
	config = Config.get().notification
	if not config.enabled:
		return
	if event not in config.events:
		return
	if len(config.endpoints) == 0:
		return

	payload = _make_payload(
		event,
		backup=backup,
		operator=operator,
		source=source,
		cost_s=cost_s,
		message=message,
		error=error,
		extra=extra,
	)

	log = logger.get()
	for endpoint in config.endpoints:
		if not endpoint.enabled:
			continue
		if len(endpoint.url) == 0:
			log.warning('Notification endpoint {} has empty url, skipped'.format(endpoint.name))
			continue
		try:
			if endpoint.type == 'bark':
				bark_url = _resolve_bark_url(endpoint)
				bark_payload = _make_bark_payload(payload, endpoint)
				_post_json(bark_url, bark_payload, endpoint.headers, endpoint.timeout.value)
			else:
				_post_json(endpoint.url, payload, endpoint.headers, endpoint.timeout.value)
			log.debug('Notification sent to {} for event {}'.format(endpoint.name, event.value))
		except Exception as e:
			log.warning('Failed to send notification to {}: {}'.format(endpoint.name, e))
