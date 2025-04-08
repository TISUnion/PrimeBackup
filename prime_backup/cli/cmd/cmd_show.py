import argparse
import dataclasses
from pathlib import Path

from typing_extensions import override

from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.cli import cli_utils
from prime_backup.cli.cmd import CliCommandHandlerBase, CommonCommandArgs, CliCommandAdapterBase
from prime_backup.types.units import ByteCount


@dataclasses.dataclass(frozen=True)
class ShowCommandArgs(CommonCommandArgs):
	backup_id: str


class ShowCommandHandler(CliCommandHandlerBase):
	def __init__(self, args: ShowCommandArgs):
		super().__init__()
		self.args = args

	def handle(self):
		self.init_environment(self.args.db_path)
		backup_id = cli_utils.parse_backup_id(self.args.backup_id)
		backup = GetBackupAction(backup_id).run()
		ss = backup.stored_size
		rs = backup.raw_size

		self.logger.info('%s', f'===== Backup #{backup.id} =====')
		self.logger.info('%s', f'ID: {backup.id}')
		self.logger.info('%s', f'Creation date: {backup.date_str}')
		self.logger.info('%s', f'Comment: {backup.comment}')
		self.logger.info('%s', f'Size (stored): {ByteCount(ss).auto_str()} ({ss}) ({100 * ss / rs:.2f}%)')
		self.logger.info('%s', f'Size (raw): {ByteCount(rs).auto_str()} ({rs})')
		self.logger.info('%s', f'Creator: type={backup.creator.type!r} name={backup.creator.name!r}')
		self.logger.info('%s', f'Tags (size={len(backup.tags)}){":" if len(backup.tags) > 0 else ""}')
		for k, v in backup.tags.items():
			self.logger.info('%s', f'  {k}: {v}')


class ShowCommandAdapter(CliCommandAdapterBase):
	@property
	@override
	def command(self) -> str:
		return 'show'

	@property
	@override
	def description(self) -> str:
		return 'Show detailed information of the given backup'

	@override
	def build_parser(self, parser: argparse.ArgumentParser):
		self._add_pos_argument_backup_id(parser)

	@override
	def run(self, args: argparse.Namespace):
		handler = ShowCommandHandler(ShowCommandArgs(
			db_path=Path(args.db),
			backup_id=args.backup_id,
		))
		handler.handle()
