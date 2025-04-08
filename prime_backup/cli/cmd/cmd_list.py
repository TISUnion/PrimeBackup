import argparse
import dataclasses
from pathlib import Path

from typing_extensions import override

from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.action.list_backup_action import ListBackupIdAction
from prime_backup.cli.cmd import CliCommandHandlerBase, CommonCommandArgs, CliCommandAdapterBase
from prime_backup.exceptions import BackupNotFound
from prime_backup.types.units import ByteCount


@dataclasses.dataclass(frozen=True)
class ListCommandArgs(CommonCommandArgs):
	human: bool


class ListCommandHandler(CliCommandHandlerBase):
	def __init__(self, args: ListCommandArgs):
		super().__init__()
		self.args = args

	def handle(self):
		self.init_environment(self.args.db_path)

		backup_ids = ListBackupIdAction().run()
		self.logger.info('Backup amount: {}'.format(len(backup_ids)))
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
			self.logger.info('%s', ' '.join([f'{k}={v}' for k, v in values.items()]))


class ListCommandAdapter(CliCommandAdapterBase):
	@property
	@override
	def command(self) -> str:
		return 'list'

	@property
	@override
	def description(self) -> str:
		return 'List backups'

	@override
	def build_parser(self, parser: argparse.ArgumentParser):
		parser.add_argument('-H', '--human', action='store_true', help='Prettify backup sizes, make it human-readable')

	@override
	def run(self, args: argparse.Namespace):
		handler = ListCommandHandler(ListCommandArgs(
			db_path=Path(args.db),
			human=args.human,
		))
		handler.handle()
