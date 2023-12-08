import argparse
import enum
import shutil
import sys
from pathlib import Path
from typing import Type

from prime_backup.action.export_backup_action import ExportBackupToDirectoryAction, ExportBackupToTarAction, ExportBackupToZipAction
from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.action.get_db_overview_action import GetDbOverviewAction
from prime_backup.action.get_file_action import GetFileAction
from prime_backup.action.import_backup_action import ImportBackupAction
from prime_backup.action.list_backup_action import ListBackupIdAction
from prime_backup.config.config import Config, set_config_instance
from prime_backup.db import db_constants
from prime_backup.db.access import DbAccess
from prime_backup.db.migration import BadDbVersion
from prime_backup.exceptions import BackupNotFound, BackupFileNotFound
from prime_backup.logger import get as get_logger
from prime_backup.types.file_info import FileType
from prime_backup.types.standalone_backup_format import StandaloneBackupFormat
from prime_backup.types.tar_format import TarFormat
from prime_backup.types.units import ByteCount

logger = get_logger()
DEFAULT_STORAGE_ROOT = Config.get_default().storage_root


def enum_options(clazz: Type[enum.Enum]) -> str:
	return ', '.join([e_.name for e_ in clazz])


class CliHandler:
	def __init__(self, args: argparse.Namespace):
		self.args = args

	def init_environment(self):
		config = Config.get_default()

		root_path = Path(self.args.db)
		if root_path.is_file() and root_path.name == db_constants.DB_FILE_NAME:
			root_path = root_path.parent

		if not (dbf := root_path / db_constants.DB_FILE_NAME).is_file():
			logger.error('Database file {!r} in does not exists'.format(dbf.as_posix()))
			sys.exit(1)

		config.storage_root = str(root_path.as_posix())
		set_config_instance(config)

		logger.info('Storage root set to {!r}'.format(config.storage_root))
		try:
			DbAccess.init(auto_migrate=False)
		except BadDbVersion as e:
			logger.info('Load database failed, you need to ensure the database is accessible with MCDR plugin: {}'.format(e))
			sys.exit(1)

	def get_ebf(self, file_path: Path) -> StandaloneBackupFormat:
		if self.args.format is None:
			if (ebf := StandaloneBackupFormat.from_file_name(file_path)) is not None:
				return ebf
			logger.error('Cannot infer export format from output file name {!r}', file_path.name)
		else:
			try:
				return StandaloneBackupFormat[self.args.format]
			except KeyError:
				logger.error('Bad format {!r}, should be one of {}'.format(self.args.format, enum_options(StandaloneBackupFormat)))
		sys.exit(1)

	def cmd_db_overview(self):
		self.init_environment()
		result = GetDbOverviewAction().run()
		logger.info('DB version: %s', result.db_version)
		logger.info('Hash method: %s', result.hash_method)
		logger.info('Backup count: %s', result.backup_cnt)
		logger.info('Blob count: %s', result.blob_cnt)
		logger.info('Blob stored size sum: %s (%s)', result.blob_stored_size_sum, ByteCount(result.blob_stored_size_sum).auto_str())
		logger.info('Blob raw size sum: %s (%s)', result.blob_raw_size_sum, ByteCount(result.blob_raw_size_sum).auto_str())
		logger.info('File count: %s', result.file_cnt)
		logger.info('File raw size sum: %s (%s)', result.file_raw_size_sum, ByteCount(result.file_raw_size_sum).auto_str())

	def cmd_show(self):
		self.init_environment()
		backup = GetBackupAction(self.args.backup_id).run()
		ss = backup.stored_size
		rs = backup.raw_size

		logger.info('%s', f'===== Backup #{backup.id} =====')
		logger.info('%s', f'ID: {backup.id}')
		logger.info('%s', f'Creation date: {backup.date_str}')
		logger.info('%s', f'Comment: {backup.comment}')
		logger.info('%s', f'Size (stored): {ByteCount(ss).auto_str()} ({ss}) ({100 * ss / rs:.2f}%)')
		logger.info('%s', f'Size (raw): {ByteCount(rs).auto_str()} ({rs})')
		logger.info('%s', f'Author: type={backup.author.type} name={backup.author.name}')
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
				**({
					'stored_size': ByteCount(backup.stored_size).auto_str() if self.args.human else backup.stored_size,
					'raw_size': ByteCount(backup.raw_size).auto_str() if self.args.human else backup.raw_size,
				} if self.args.size else {}),
				'author': repr(str(backup.author)),
				'comment': repr(backup.comment)
			}
			logger.info('%s', ' '.join([f'{k}={v}' for k, v in values.items()]))

	def cmd_import(self):
		input_path = Path(self.args.input)
		fmt = self.get_ebf(input_path)
		self.init_environment()

		logger.info('Importing backup from {}, format {}'.format(str(input_path.as_posix()), fmt.name))
		ImportBackupAction(input_path, fmt).run()

	def cmd_export(self):
		output_path = Path(self.args.output)
		fmt = self.get_ebf(output_path)
		self.init_environment()

		backup = GetBackupAction(self.args.backup_id).run()
		logger.info('Exporting backup #{} to {}, format {}'.format(backup.id, str(output_path.as_posix()), fmt.name))
		kwargs = dict(fail_soft=self.args.fail_soft, verify_blob=not self.args.no_verify)
		if isinstance(fmt.value, TarFormat):
			act = ExportBackupToTarAction(backup.id, output_path, fmt.value, **kwargs)
		else:
			act = ExportBackupToZipAction(backup.id, output_path, **kwargs)

		failures = act.run()
		if len(failures) > 0:
			logger.warning('Found {} failures during the export'.format(len(failures)))
			for failure in failures:
				logger.warning('  {} mode={}: {} {}'.format(failure.file.path, oct(failure.file.mode), type(failure.error), str(failure.error)))

	def cmd_extract(self):
		file_path = Path(self.args.file)
		output_path = Path(self.args.output)
		self.init_environment()

		file = GetFileAction(self.args.backup_id, file_path).run()
		logger.info('Found file {}'.format(file))
		if self.args.type is not None:
			try:
				ft = FileType[self.args.type]
			except KeyError:
				logger.error('Bad file type {!r}, should be one of {}'.format(self.args.type, enum_options(FileType)))
				sys.exit(1)
			else:
				if ft != file.file_type:
					logger.error('Unexpected file type, expected {}, but found {} for the to-be-extracted file'.format(self.args.type, file.file_type.name))
					sys.exit(2)

		dest_path = output_path / file_path.name
		if dest_path.is_file():
			dest_path.unlink()
		elif dest_path.is_dir():
			shutil.rmtree(dest_path)

		ExportBackupToDirectoryAction(
			self.args.backup_id, output_path,
			delete_existing=True,
			child_to_export=file_path,
			recursively_export_child=self.args.recursively,
		).run()

	@classmethod
	def entrypoint(cls):
		parser = argparse.ArgumentParser(description='Prime Backup CLI tools', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
		parser.add_argument('-d', '--db', default=DEFAULT_STORAGE_ROOT, help='Path to the {db} database file, or path to the directory that contains the {db} database file, e.g. "/my/path/{db}", or "/my/path"'.format(db=db_constants.DB_FILE_NAME))
		subparsers = parser.add_subparsers(title='Command', help='Available commands', dest='command')

		desc = 'Show overview information of the database'
		parser_overview = subparsers.add_parser('overview', help=desc, description=desc)

		desc = 'List backups'
		parser_list = subparsers.add_parser('list', help=desc, description=desc, add_help=False)
		parser_list.add_argument('--help', action='store_true', help='show this help message and exit')
		parser_list.add_argument('-s', '--size', action='store_true', help='Show backup sizes')
		parser_list.add_argument('-h', '--human', action='store_true', help='Prettify backup sizes, make it human-readable')

		desc = 'Show detailed information of the given backup'
		parser_show = subparsers.add_parser('show', help=desc, description=desc)
		parser_show.add_argument('backup_id', type=int, help='The ID of the backup to export')

		desc = 'Import a backup from the given file'
		parser_import = subparsers.add_parser('import', help=desc, description=desc)
		parser_import.add_argument('input', help='The file name of the backup to be imported. Example: my_backup.tar')
		parser_import.add_argument('-f', '--format', help='The format of the input file. If not given, attempt to infer from the input file name. Options: {}'.format(enum_options(StandaloneBackupFormat)))

		desc = 'Export the given backup to a single file'
		parser_export = subparsers.add_parser('export', help=desc, description=desc)
		parser_export.add_argument('backup_id', type=int, help='The ID of the backup to export')
		parser_export.add_argument('output', help='The output file name of the exported backup. Example: my_backup.tar')
		parser_export.add_argument('-f', '--format', help='The format of the output file. If not given, attempt to infer from the output file name. Options: {}'.format(enum_options(StandaloneBackupFormat)))
		parser_export.add_argument('--fail-soft', action='store_true', help='Skip files with export failure in the backup, so a single failure will not abort the export')
		parser_export.add_argument('--no-verify', action='store_true', help='Do not verify the exported file contents')

		desc = 'Extract a single file from a backup'
		parser_extract = subparsers.add_parser('extract', help=desc, description=desc)
		parser_extract.add_argument('backup_id', type=int, help='The ID of the backup to export')
		parser_extract.add_argument('file', help='The related path of the to-be-extracted file inside the backup')
		parser_extract.add_argument('output', help='The output file name of the extracted file')
		parser_extract.add_argument('-r', '--recursively', action='store_true', help='If the file to extract is a directory, recursively extract all of its containing files')
		parser_extract.add_argument('-t', '--type', help='Type assertion of the extracted file. Default: no assertion. Options: {}'.format(enum_options(FileType)))

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
				if args.help:
					parser_list.print_help()
				else:
					handler.cmd_list()
			elif args.command == 'import':
				handler.cmd_import()
			elif args.command == 'export':
				handler.cmd_export()
			elif args.command == 'extract':
				handler.cmd_extract()
			else:
				logger.error('Unknown command {!r}'.format(args.command))
		except BackupNotFound as e:
			logger.error('Backup #{} does not exist'.format(e.backup_id))
		except BackupFileNotFound as e:
			logger.error('File {!r} in backup #{} does not exist'.format(e.path, e.backup_id))


def cli_entry():
	CliHandler.entrypoint()
