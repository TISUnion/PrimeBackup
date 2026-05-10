import argparse
import dataclasses
from pathlib import Path

from typing_extensions import override

from prime_backup.action.create_backup_action import CreateBackupAction
from prime_backup.cli.cmd import CliCommandHandlerBase, CommonCommandArgs, CliCommandAdapterBase
from prime_backup.types.operator import Operator, PrimeBackupOperatorNames
from prime_backup.utils.timer import Timer


@dataclasses.dataclass(frozen=True)
class MakeCommandArgs(CommonCommandArgs):
	comment: str
	source_root: Path


class MakeCommandHandler(CliCommandHandlerBase):
	def __init__(self, args: MakeCommandArgs):
		super().__init__()
		self.args = args

	def handle(self):
		self.init_environment_from_args(self.args)

		timer = Timer()
		self.logger.info('Creating backup at path {!r}, targets: {}, comment: {!r}'.format(
			self.args.source_root.as_posix(), self.config.backup.targets, self.args.comment,
		))
		backup = CreateBackupAction(
			creator=Operator.pb(PrimeBackupOperatorNames.cli),
			comment=self.args.comment,
			source_path=self.args.source_root,
		).run()
		cost_create = timer.get_elapsed()
		self.logger.info('Create backup #{} done, cost {}s'.format(backup.id, round(cost_create, 2)))


class MakeCommandAdapter(CliCommandAdapterBase):
	@property
	@override
	def command(self) -> str:
		return 'make'

	@property
	@override
	def description(self) -> str:
		return 'Create a new backup'

	@override
	def build_parser(self, parser: argparse.ArgumentParser):
		parser.add_argument('comment', nargs='?', default='', help='The comment of the backup')
		parser.add_argument('-s', '--source-root', required=True, help='The source root directory to create backup from')

	@override
	def run(self, args: argparse.Namespace):
		handler = MakeCommandHandler(MakeCommandArgs(
			db_path=Path(args.db),
			config_path=Path(args.config) if args.config is not None else None,
			comment=args.comment,
			source_root=Path(args.source_root),
		))
		handler.handle()
