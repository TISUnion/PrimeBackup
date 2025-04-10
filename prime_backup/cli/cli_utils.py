import enum
import functools
import json
import zipfile
from pathlib import Path
from typing import Optional, Type

from prime_backup import logger
from prime_backup.cli.return_codes import ErrorReturnCodes
from prime_backup.db.access import DbAccess
from prime_backup.types.standalone_backup_format import StandaloneBackupFormat
from prime_backup.utils.backup_id_parser import BackupIdParser


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
	if not DbAccess.is_initialized():
		raise RuntimeError('DB not initialized')
	return BackupIdParser(allow_db_access=True).parse(value)


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
	ErrorReturnCodes.invalid_argument.sys_exit()
