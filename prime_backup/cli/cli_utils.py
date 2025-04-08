import contextlib
import enum
import functools
import json
import sys
import zipfile
from pathlib import Path
from typing import Optional, Type

from prime_backup import logger
from prime_backup.action.list_backup_action import ListBackupIdAction
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.types.standalone_backup_format import StandaloneBackupFormat


class BackupIdAlternatives(enum.Enum):
	latest = enum.auto()
	latest_with_temp = enum.auto()


def enum_options(clazz: Type[enum.Enum]) -> str:
	return ', '.join([e_.name for e_ in clazz])


@functools.lru_cache(None)
def get_plugin_version() -> str:
	root = Path(__file__).parent.parent.parent
	#      cli_utils.py    cli   pb pkg  root

	meta_file_name = 'mcdreforged.plugin.json'
	try:
		if root.is_file():
			with zipfile.ZipFile(root) as z, z.open(meta_file_name) as f:
				meta = json.load(f)
		elif root.is_dir():
			with open(meta_file_name, 'rb') as f:
				meta = json.load(f)
		else:
			raise Exception('unknown file type {!r}'.format(root))
	except Exception as e:
		logger.get().error('Failed to get plugin version: {}'.format(e))
		return '?'
	else:
		return meta['version']


def parse_backup_id(value: str) -> int:
	"""
	Notes: DB should have been initialized
	"""
	with contextlib.suppress(ValueError):
		return int(value)

	alt = BackupIdAlternatives[value.lower()]
	if alt in [BackupIdAlternatives.latest, BackupIdAlternatives.latest_with_temp]:
		backup_filter = BackupFilter()
		if alt == BackupIdAlternatives.latest:
			backup_filter.filter_non_temporary_backup()

		candidates = ListBackupIdAction(backup_filter=backup_filter, limit=1).run()
		if len(candidates) == 0:
			raise ValueError('found no backup in the database')

		backup_id = candidates[0]
		logger.get().info('Found latest non-temp backup #{}'.format(backup_id))
		return backup_id

	raise ValueError('unsupported backup alternative {!r}'.format(alt))


def get_ebf(file_path: Path, format_: Optional[str]) -> StandaloneBackupFormat:
	if format_ is None:
		if (ebf := StandaloneBackupFormat.from_file_name(file_path)) is not None:
			return ebf
		logger.get().error('Cannot infer export format from output file name {!r}'.format(file_path.name))
	else:
		try:
			return StandaloneBackupFormat[format_]
		except KeyError:
			logger.get().error('Bad format {!r}, should be one of {}'.format(format_, enum_options(StandaloneBackupFormat)))
	sys.exit(1)
