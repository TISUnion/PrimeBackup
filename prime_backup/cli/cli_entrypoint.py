import argparse
from typing import List, Dict

from prime_backup.cli import cli_utils
from prime_backup.cli.cmd import CliCommandAdapterBase
from prime_backup.cli.cmd.cmd_db_overview import DbOverviewCommandAdapter
from prime_backup.cli.cmd.cmd_export import ExportCommandAdapter
from prime_backup.cli.cmd.cmd_extract import ExtractCommandAdapter
from prime_backup.cli.cmd.cmd_import import ImportCommandAdapter
from prime_backup.cli.cmd.cmd_list import ListCommandAdapter
from prime_backup.cli.cmd.cmd_migrate_db import MigrateDbCommandAdapter
from prime_backup.cli.cmd.cmd_show import ShowCommandAdapter
from prime_backup.cli.return_codes import ErrorReturnCodes
from prime_backup.config.config import Config
from prime_backup.db import db_constants
from prime_backup.exceptions import BackupNotFound, BackupFileNotFound
from prime_backup.logger import get as get_logger
from prime_backup.utils import log_utils

__all__ = ['cli_entry']


def __prepare_logger():
	logger = get_logger()
	assert len(logger.handlers) == 1
	logger.handlers[0].setFormatter(log_utils.LOG_FORMATTER_NO_FUNC)


__prepare_logger()
DEFAULT_STORAGE_ROOT = Config.get_default().storage_root


class CliEntrypoint:
	def __init__(self):
		self.logger = get_logger()
		self.adaptors = self.__create_command_adapters()

	@classmethod
	def __create_command_adapters(cls) -> Dict[str, CliCommandAdapterBase]:
		all_adapters: List[CliCommandAdapterBase] = [
			DbOverviewCommandAdapter(),
			ExportCommandAdapter(),
			ExtractCommandAdapter(),
			ImportCommandAdapter(),
			ListCommandAdapter(),
			MigrateDbCommandAdapter(),
			ShowCommandAdapter(),
		]
		adaptor_by_command = {adapter.command: adapter for adapter in all_adapters}

		if len(all_adapters) != len(adaptor_by_command):
			raise AssertionError(all_adapters, adaptor_by_command)

		return adaptor_by_command

	def main(self):
		parser = argparse.ArgumentParser(description='Prime Backup v{} CLI tools'.format(cli_utils.get_plugin_version()), formatter_class=argparse.ArgumentDefaultsHelpFormatter)
		parser.add_argument('-d', '--db', default=DEFAULT_STORAGE_ROOT, help='Path to the {db} database file, or path to the directory that contains the {db} database file, e.g. "/my/path/{db}", or "/my/path"'.format(db=db_constants.DB_FILE_NAME))
		subparsers = parser.add_subparsers(title='Command', help='Available commands', dest='command')

		for adapter in self.adaptors.values():
			subparser = subparsers.add_parser(adapter.command, help=adapter.description, description=adapter.description)
			adapter.build_parser(subparser)

		args = parser.parse_args()
		if args.command is None:
			parser.print_help()
			return

		adapter = self.adaptors.get(args.command)
		if adapter is None:
			self.logger.error('Unknown command {!r}'.format(args.command))
			ErrorReturnCodes.invalid_argument.sys_exit()

		try:
			adapter.run(args)
		except BackupNotFound as e:
			self.logger.error('Backup #{} does not exist'.format(e.backup_id))
			ErrorReturnCodes.backup_not_found.sys_exit()
		except BackupFileNotFound as e:
			self.logger.error('File {!r} in backup #{} does not exist'.format(e.path, e.backup_id))
			ErrorReturnCodes.backup_file_not_found.sys_exit()


def cli_entry():
	CliEntrypoint().main()
