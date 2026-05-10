import argparse
import dataclasses
from pathlib import Path

from typing_extensions import override

from prime_backup.action.get_db_overview_action import GetDbOverviewAction
from prime_backup.cli.cmd import CliCommandHandlerBase, CommonCommandArgs, CliCommandAdapterBase
from prime_backup.cli.return_codes import ErrorReturnCodes
from prime_backup.config.config import set_config_instance
from prime_backup.db import db_constants
from prime_backup.db.access import DbAccess


@dataclasses.dataclass(frozen=True)
class InitCommandArgs(CommonCommandArgs):
	pass


class InitCommandHandler(CliCommandHandlerBase):
	def __init__(self, args: InitCommandArgs):
		super().__init__()
		self.args = args

	def handle(self):
		root_path = self.args.db_path
		if root_path.name == db_constants.DB_FILE_NAME and not root_path.is_dir():
			self.logger.error('The init command expects --db to be the database parent directory, not the database file path {!r}'.format(root_path.as_posix()))
			ErrorReturnCodes.invalid_argument.sys_exit()

		db_file_path = root_path / db_constants.DB_FILE_NAME
		if db_file_path.exists():
			self.logger.error('Database file {!r} already exists'.format(db_file_path.as_posix()))
			ErrorReturnCodes.invalid_argument.sys_exit()

		config = self.load_config(root_path, self.args.config_path)
		set_config_instance(config)
		self.init_db(root_path, create=True, migrate=False)

		result = GetDbOverviewAction().run()
		self.logger.info('Database initialized at {!r}, current DB version: {}'.format(DbAccess.get_db_file_path().as_posix(), result.db_version))


class InitCommandAdapter(CliCommandAdapterBase):
	@property
	@override
	def command(self) -> str:
		return 'init'

	@property
	@override
	def description(self) -> str:
		return 'Initialize a new database at the directory given by --db'

	@override
	def build_parser(self, parser: argparse.ArgumentParser):
		pass

	@override
	def run(self, args: argparse.Namespace):
		handler = InitCommandHandler(InitCommandArgs(
			db_path=Path(args.db),
			config_path=Path(args.config) if args.config is not None else None,
		))
		handler.handle()
