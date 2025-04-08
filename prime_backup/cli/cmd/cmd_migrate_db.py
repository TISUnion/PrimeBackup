import argparse
import dataclasses
from pathlib import Path

from typing_extensions import override

from prime_backup.action.get_db_overview_action import GetDbOverviewAction
from prime_backup.cli.cmd import CliCommandHandlerBase, CommonCommandArgs, CliCommandAdapterBase
from prime_backup.db import db_constants


@dataclasses.dataclass(frozen=True)
class MigrateDbCommandArgs(CommonCommandArgs):
	pass


class MigrateDbCommandHandler(CliCommandHandlerBase):
	def __init__(self, args: MigrateDbCommandArgs):
		super().__init__()
		self.args = args

	def handle(self):
		self.init_environment(self.args.db_path, migrate=True)
		result = GetDbOverviewAction().run()
		self.logger.info('Migration done, current DB version: {}'.format(result.db_version))


class MigrateDbCommandAdapter(CliCommandAdapterBase):
	@property
	@override
	def command(self) -> str:
		return 'migrate_db'

	@property
	@override
	def description(self) -> str:
		return 'Migrate the database to the current version {}'.format(db_constants.DB_VERSION)

	@override
	def build_parser(self, parser: argparse.ArgumentParser):
		pass

	@override
	def run(self, args: argparse.Namespace):
		handler = MigrateDbCommandHandler(MigrateDbCommandArgs(
			db_path=Path(args.db),
		))
		handler.handle()
