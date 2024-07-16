import argparse
import contextlib
import enum
import functools
import json
import sys
import zipfile
from pathlib import Path
from typing import Type

from prime_backup import constants
from prime_backup.action.export_backup_action import ExportBackupToDirectoryAction, ExportBackupToTarAction, \
	ExportBackupToZipAction
from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.action.get_db_overview_action import GetDbOverviewAction
from prime_backup.action.get_file_action import GetFileAction
from prime_backup.action.import_backup_action import ImportBackupAction, BackupMetadataNotFound
from prime_backup.action.list_backup_action import ListBackupIdAction
from prime_backup.config.config import Config, set_config_instance
from prime_backup.db import db_constants
from prime_backup.db.access import DbAccess
from prime_backup.db.migration import BadDbVersion
from prime_backup.exceptions import BackupNotFound, BackupFileNotFound
from prime_backup.logger import get as get_logger
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.types.standalone_backup_format import StandaloneBackupFormat
from prime_backup.types.tar_format import TarFormat
from prime_backup.types.units import ByteCount
from prime_backup.utils import log_utils

__all__ = ['cli_entry']

logger = get_logger()
assert len(logger.handlers) == 1
logger.handlers[0].setFormatter(log_utils.LOG_FORMATTER_NO_FUNC)
DEFAULT_STORAGE_ROOT = Config.get_default().storage_root


class ErrorReturnCodes(enum.Enum):
	argparse_error = 2  # see argparse.ArgumentParser.error
	action_failed = 3
	backup_not_found = 4
	backup_file_not_found = 5

	def sys_exit(self):
		sys.exit(self.value)


class BackupIdAlternatives(enum.Enum):
	latest = enum.auto()
	latest_non_temp = enum.auto()


def enum_options(clazz: Type[enum.Enum]) -> str:
	return ', '.join([e_.name for e_ in clazz])


@functools.lru_cache
def _get_plugin_version() -> str:
	root = Path(__file__).parent.parent.parent
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
		logger.error('Failed to get plugin version: {}'.format(e))
		return '?'
	else:
		return meta['version']


class CliHandler:
	def __init__(self, args: argparse.Namespace):
		self.args = args

	def init_environment(self, *, migrate: bool = False):
		config = Config.get_default()
		set_config_instance(config)

		root_path = Path(self.args.db)
		if root_path.is_file() and root_path.name == db_constants.DB_FILE_NAME:
			root_path = root_path.parent

		if not (dbf := root_path / db_constants.DB_FILE_NAME).is_file():
			logger.error('Database file {!r} does not exist'.format(dbf.as_posix()))
			sys.exit(1)
		config.storage_root = str(root_path.as_posix())

		logger.info('Storage root set to {!r}'.format(config.storage_root))
		try:
			DbAccess.init(create=False, migrate=migrate)
		except BadDbVersion as e:
			logger.info('Load database failed, you need to ensure the database is accessible with MCDR plugin: {}'.format(e))
			sys.exit(1)
		config.backup.hash_method = DbAccess.get_hash_method()  # use the hash method from the db

	def get_ebf(self, file_path: Path) -> StandaloneBackupFormat:
		if self.args.format is None:
			if (ebf := StandaloneBackupFormat.from_file_name(file_path)) is not None:
				return ebf
			logger.error('Cannot infer export format from output file name {!r}'.format(file_path.name))
		else:
			try:
				return StandaloneBackupFormat[self.args.format]
			except KeyError:
				logger.error('Bad format {!r}, should be one of {}'.format(self.args.format, enum_options(StandaloneBackupFormat)))
		sys.exit(1)

	@classmethod
	def __parse_backup_id(cls, value: str) -> int:
		with contextlib.suppress(ValueError):
			return int(value)

		alt = BackupIdAlternatives[value.lower()]
		if alt in [BackupIdAlternatives.latest, BackupIdAlternatives.latest_non_temp]:
			backup_filter = BackupFilter()
			if alt == BackupIdAlternatives.latest:
				backup_filter.filter_non_temporary_backup()

			candidates = ListBackupIdAction(backup_filter=backup_filter, limit=1).run()
			if len(candidates) == 0:
				raise ValueError('found no backup in the database')

			backup_id = candidates[0]
			logger.info('Found latest non-temp backup #{}'.format(backup_id))
			return backup_id

		raise ValueError('unsupported backup alternative {!r}'.format(alt))

	def cmd_db_overview(self):
		self.init_environment()
		result = GetDbOverviewAction().run()
		logger.info('DB version: %s', result.db_version)
		logger.info('DB path: %s', DbAccess.get_db_file_path())
		logger.info('DB file size: %s (%s)', result.db_file_size, ByteCount(result.db_file_size).auto_str())
		logger.info('Hash method: %s', result.hash_method)
		logger.info('Backup count: %s', result.backup_count)
		logger.info('Blob count: %s', result.blob_count)
		logger.info('Blob stored size sum: %s (%s)', result.blob_stored_size_sum, ByteCount(result.blob_stored_size_sum).auto_str())
		logger.info('Blob raw size sum: %s (%s)', result.blob_raw_size_sum, ByteCount(result.blob_raw_size_sum).auto_str())
		logger.info('File count: %s', result.file_count)
		logger.info('File raw size sum: %s (%s)', result.file_raw_size_sum, ByteCount(result.file_raw_size_sum).auto_str())

	def cmd_show(self):
		self.init_environment()
		backup_id = self.__parse_backup_id(self.args.backup_id)
		backup = GetBackupAction(backup_id).run()
		ss = backup.stored_size
		rs = backup.raw_size

		logger.info('%s', f'===== Backup #{backup.id} =====')
		logger.info('%s', f'ID: {backup.id}')
		logger.info('%s', f'Creation date: {backup.date_str}')
		logger.info('%s', f'Comment: {backup.comment}')
		logger.info('%s', f'Size (stored): {ByteCount(ss).auto_str()} ({ss}) ({100 * ss / rs:.2f}%)')
		logger.info('%s', f'Size (raw): {ByteCount(rs).auto_str()} ({rs})')
		logger.info('%s', f'Creator: type={backup.creator.type!r} name={backup.creator.name!r}')
		logger.info('%s', f'Tags (size={len(backup.tags)}){":" if len(backup.tags) > 0 else ""}')
		for k, v in backup.tags.items():
			logger.info('%s', f'  {k}: {v}')

	def cmd_list(self):
		self.init_environment()

		backup_ids = ListBackupIdAction().run()
		logger.info('Backup amount: {}'.format(len(backup_ids)))
		for backup_id in backup_ids:
			try:
				backup = GetBackupAction(backup_id).run()
			except BackupNotFound:
				continue
			values = {
				'id': backup.id,
				'date': repr(backup.date_str),
				'stored_size': ByteCount(backup.stored_size).auto_str() if self.args.human else backup.stored_size,
				'raw_size': ByteCount(backup.raw_size).auto_str() if self.args.human else backup.raw_size,
				'creator': repr(str(backup.creator)),
				'comment': repr(backup.comment)
			}
			logger.info('%s', ' '.join([f'{k}={v}' for k, v in values.items()]))

	def cmd_import(self):
		input_path = Path(self.args.input)
		fmt = self.get_ebf(input_path)
		self.init_environment()

		if self.args.meta_override is not None:
			try:
				meta_override = json.loads(self.args.meta_override)
			except ValueError as e:
				logger.error('Bad json {!r}: {}'.format(self.args.meta_override, e))
				sys.exit(1)
			if not isinstance(meta_override, dict):
				logger.error('meta_override should be a dict, but found {}: {!r}'.format(type(meta_override), meta_override))
				sys.exit(1)
		else:
			meta_override = None

		logger.info('Importing backup from {}, format: {}'.format(str(input_path.as_posix()), fmt.name))
		try:
			ImportBackupAction(input_path, fmt, ensure_meta=not self.args.auto_meta, meta_override=meta_override).run()
		except BackupMetadataNotFound as e:
			logger.error('Import failed due to backup metadata not found: {}'.format(e))
			logger.error('Please make sure the file is a valid backup create by Prime Backup. You can also use the --auto-meta flag for a workaround')
			ErrorReturnCodes.action_failed.sys_exit()

	def cmd_export(self):
		output_path = Path(self.args.output)
		fmt = self.get_ebf(output_path)
		self.init_environment()

		backup_id = self.__parse_backup_id(self.args.backup_id)
		backup = GetBackupAction(backup_id).run()
		logger.info('Exporting backup #{} to {}, format {}'.format(backup.id, str(output_path.as_posix()), fmt.name))
		kwargs = dict(
			fail_soft=self.args.fail_soft,
			verify_blob=not self.args.no_verify,
			create_meta=not self.args.no_meta,
		)
		if isinstance(fmt.value, TarFormat):
			act = ExportBackupToTarAction(backup.id, output_path, fmt.value, **kwargs)
		else:
			act = ExportBackupToZipAction(backup.id, output_path, **kwargs)

		failures = act.run()
		if len(failures) > 0:
			logger.warning('Found {} failures during the export'.format(len(failures)))
			for line in failures.to_lines():
				logger.warning('  {}'.format(line.to_plain_text()))
			ErrorReturnCodes.action_failed.sys_exit()

	def cmd_extract(self):
		file_path = Path(self.args.file)
		output_path = Path(self.args.output)
		self.init_environment()

		backup_id = self.__parse_backup_id(self.args.backup_id)

		if file_path != Path('.'):
			file = GetFileAction(backup_id, file_path).run()
			logger.info('Found file {}'.format(file))

		failures = ExportBackupToDirectoryAction(
			backup_id, output_path,
			child_to_export=file_path,
			recursively_export_child=self.args.recursively,
		).run()
		if len(failures) > 0:
			logger.warning('Found {} failures during the extract'.format(len(failures)))
			for line in failures.to_lines():
				logger.warning('  {}'.format(line.to_plain_text()))
			ErrorReturnCodes.action_failed.sys_exit()

	def cmd_migrate_db(self):
		self.init_environment(migrate=True)
		result = GetDbOverviewAction().run()
		logger.info('Current DB version: {}'.format(result.db_version))

	@classmethod
	def entrypoint(cls):
		parser = argparse.ArgumentParser(description='Prime Backup v{} CLI tools'.format(_get_plugin_version()), formatter_class=argparse.ArgumentDefaultsHelpFormatter)
		parser.add_argument('-d', '--db', default=DEFAULT_STORAGE_ROOT, help='Path to the {db} database file, or path to the directory that contains the {db} database file, e.g. "/my/path/{db}", or "/my/path"'.format(db=db_constants.DB_FILE_NAME))
		subparsers = parser.add_subparsers(title='Command', help='Available commands', dest='command')

		def add_pos_argument_backup_id(p: argparse.ArgumentParser):
			def backup_id(s: str):
				with contextlib.suppress(ValueError):
					return int(s)
				with contextlib.suppress(KeyError):
					_ = BackupIdAlternatives[s.lower()]
					return s
				raise ValueError()

			p.add_argument('backup_id', type=backup_id, help='The ID of the backup to export. Besides an integer ID, it can also be "latest" and "latest_non_temp"')

		desc = 'Show overview information of the database'
		parser_overview = subparsers.add_parser('overview', help=desc, description=desc)

		desc = 'List backups'
		parser_list = subparsers.add_parser('list', help=desc, description=desc)
		parser_list.add_argument('-H', '--human', action='store_true', help='Prettify backup sizes, make it human-readable')

		desc = 'Show detailed information of the given backup'
		parser_show = subparsers.add_parser('show', help=desc, description=desc)
		add_pos_argument_backup_id(parser_show)

		desc = 'Import a backup from the given file. The backup file needs to have a backup metadata file {!r}, or the --auto-meta flag need to be supplied'.format(constants.BACKUP_META_FILE_NAME)
		parser_import = subparsers.add_parser('import', help=desc, description=desc)
		parser_import.add_argument('input', help='The file name of the backup to be imported. Example: my_backup.tar')
		parser_import.add_argument('-f', '--format', help='The format of the input file. If not given, attempt to infer from the input file name. Options: {}'.format(enum_options(StandaloneBackupFormat)))
		parser_import.add_argument('--auto-meta', action='store_true', help='If the backup metadata file does not exist, create an auto-generated one based on the file content')
		parser_import.add_argument('--meta-override', help='An optional json object string. It overrides the metadata of the imported backup, regardless of whether the backup metadata file exists or not')

		desc = 'Export the given backup to a single file'
		parser_export = subparsers.add_parser('export', help=desc, description=desc)
		add_pos_argument_backup_id(parser_export)
		parser_export.add_argument('output', help='The output file name of the exported backup. Example: my_backup.tar')
		parser_export.add_argument('-f', '--format', help='The format of the output file. If not given, attempt to infer from the output file name. Options: {}'.format(enum_options(StandaloneBackupFormat)))
		parser_export.add_argument('--fail-soft', action='store_true', help='Skip files with export failure in the backup, so a single failure will not abort the export. Notes: a corrupted file might damaged the tar-based file ')
		parser_export.add_argument('--no-verify', action='store_true', help='Do not verify the exported file contents')
		parser_export.add_argument('--no-meta', action='store_true', help='Do not add the backup metadata file {!r} in the exported file'.format(constants.BACKUP_META_FILE_NAME))

		desc = 'Extract a single file / directory from a backup'
		parser_extract = subparsers.add_parser('extract', help=desc, description=desc)
		add_pos_argument_backup_id(parser_extract)
		parser_extract.add_argument('file', help='The related path of the to-be-extracted file inside the backup. Use "." to extract everything in the backup')
		parser_extract.add_argument('-o', '--output', default='.', help='The output directory to place the extracted file / directory')
		parser_extract.add_argument('-r', '--recursively', action='store_true', help='If the file to extract is a directory, recursively extract all of its containing files')

		desc = 'Migrate the database to the current version {}'.format(db_constants.DB_VERSION)
		parser_migrate_db = subparsers.add_parser('migrate_db', help=desc, description=desc)

		args = parser.parse_args()
		if args.command is None:
			parser.print_help()
			return

		handler = CliHandler(args)
		try:
			if args.command == 'overview':
				handler.cmd_db_overview()
			elif args.command == 'show':
				handler.cmd_show()
			elif args.command == 'list':
				handler.cmd_list()
			elif args.command == 'import':
				handler.cmd_import()
			elif args.command == 'export':
				handler.cmd_export()
			elif args.command == 'extract':
				handler.cmd_extract()
			elif args.command == 'migrate_db':
				handler.cmd_migrate_db()
			else:
				logger.error('Unknown command {!r}'.format(args.command))
		except BackupNotFound as e:
			logger.error('Backup #{} does not exist'.format(e.backup_id))
			ErrorReturnCodes.backup_not_found.sys_exit()
		except BackupFileNotFound as e:
			logger.error('File {!r} in backup #{} does not exist'.format(e.path, e.backup_id))
			ErrorReturnCodes.backup_file_not_found.sys_exit()


def cli_entry():
	CliHandler.entrypoint()
