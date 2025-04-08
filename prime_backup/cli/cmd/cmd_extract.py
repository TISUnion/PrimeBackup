import argparse
import dataclasses
from pathlib import Path

from typing_extensions import override

from prime_backup.action.export_backup_action_directory import ExportBackupToDirectoryAction
from prime_backup.action.get_file_action import GetBackupFileAction
from prime_backup.cli import cli_utils
from prime_backup.cli.cmd import CliCommandHandlerBase, CommonCommandArgs, CliCommandAdapterBase
from prime_backup.cli.return_codes import ErrorReturnCodes


@dataclasses.dataclass(frozen=True)
class ExtractCommandArgs(CommonCommandArgs):
	backup_id: str
	file_path: Path
	output_path: Path
	recursively: bool


class ExtractCommandHandler(CliCommandHandlerBase):
	def __init__(self, args: ExtractCommandArgs):
		super().__init__()
		self.args = args

	def handle(self):
		self.init_environment(self.args.db_path)

		backup_id = cli_utils.parse_backup_id(self.args.backup_id)

		if self.args.file_path != Path('.'):
			file = GetBackupFileAction(backup_id, self.args.file_path).run()
			self.logger.info('Found file {}'.format(file))

		failures = ExportBackupToDirectoryAction(
			backup_id, self.args.output_path,
			child_to_export=self.args.file_path,
			recursively_export_child=self.args.recursively,
		).run()
		if len(failures) > 0:
			self.logger.warning('Found {} failures during the extract'.format(len(failures)))
			for line in failures.to_lines():
				self.logger.warning('  {}'.format(line.to_plain_text()))
			ErrorReturnCodes.action_failed.sys_exit()


class ExtractCommandAdapter(CliCommandAdapterBase):
	@property
	@override
	def command(self) -> str:
		return 'extract'

	@property
	@override
	def description(self) -> str:
		return 'Extract a single file / directory from a backup'

	@override
	def build_parser(self, parser: argparse.ArgumentParser):
		self._add_pos_argument_backup_id(parser)
		parser.add_argument('file', help='The related path of the to-be-extracted file inside the backup. Use "." to extract everything in the backup')
		parser.add_argument('-o', '--output', default='.', help='The output directory to place the extracted file / directory')
		parser.add_argument('-r', '--recursively', action='store_true', help='If the file to extract is a directory, recursively extract all of its containing files')

	@override
	def run(self, args: argparse.Namespace):
		handler = ExtractCommandHandler(ExtractCommandArgs(
			db_path=Path(args.db),
			backup_id=args.backup_id,
			file_path=Path(args.file),
			output_path=Path(args.output),
			recursively=args.recursively,
		))
		handler.handle()
