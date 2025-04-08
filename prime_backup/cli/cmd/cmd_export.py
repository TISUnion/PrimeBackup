import argparse
import dataclasses
from pathlib import Path
from typing import Optional

from typing_extensions import override

from prime_backup import constants
from prime_backup.action.export_backup_action_base import ExportBackupActionCommonInitKwargs
from prime_backup.action.export_backup_action_tar import ExportBackupToTarAction
from prime_backup.action.export_backup_action_zip import ExportBackupToZipAction
from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.cli import cli_utils
from prime_backup.cli.cmd import CliCommandHandlerBase, CommonCommandArgs, CliCommandAdapterBase
from prime_backup.cli.return_codes import ErrorReturnCodes
from prime_backup.types.standalone_backup_format import StandaloneBackupFormat
from prime_backup.types.tar_format import TarFormat


@dataclasses.dataclass(frozen=True)
class ExportCommandArgs(CommonCommandArgs):
	backup_id: str
	output_path: Path
	format: Optional[str]
	fail_soft: bool
	no_verify: bool
	no_meta: bool


class ExportCommandHandler(CliCommandHandlerBase):
	def __init__(self, args: ExportCommandArgs):
		super().__init__()
		self.args = args

	def handle(self):
		fmt = cli_utils.get_ebf(self.args.output_path, self.args.format)
		self.init_environment(self.args.db_path)

		backup_id = cli_utils.parse_backup_id(self.args.backup_id)
		backup = GetBackupAction(backup_id).run()
		self.logger.info('Exporting backup #{} to {}, format {}'.format(backup.id, str(self.args.output_path.as_posix()), fmt.name))
		kwargs: ExportBackupActionCommonInitKwargs = dict(
			fail_soft=self.args.fail_soft,
			verify_blob=not self.args.no_verify,
			create_meta=not self.args.no_meta,
		)
		if isinstance(fmt.value, TarFormat):
			act = ExportBackupToTarAction(backup.id, self.args.output_path, fmt.value, **kwargs)
		else:
			act = ExportBackupToZipAction(backup.id, self.args.output_path, **kwargs)

		failures = act.run()
		if len(failures) > 0:
			self.logger.warning('Found {} failures during the export'.format(len(failures)))
			for line in failures.to_lines():
				self.logger.warning('  {}'.format(line.to_plain_text()))
			ErrorReturnCodes.action_failed.sys_exit()


class ExportCommandAdapter(CliCommandAdapterBase):
	@property
	@override
	def command(self) -> str:
		return 'export'

	@property
	@override
	def description(self) -> str:
		return 'Export the given backup to a single file'

	@override
	def build_parser(self, parser: argparse.ArgumentParser):
		self._add_pos_argument_backup_id(parser)
		parser.add_argument('output', help='The output file name of the exported backup. Example: my_backup.tar')
		parser.add_argument('-f', '--format', help='The format of the output file. If not given, attempt to infer from the output file name. Options: {}'.format(cli_utils.enum_options(StandaloneBackupFormat)))
		parser.add_argument('--fail-soft', action='store_true', help='Skip files with export failure in the backup, so a single failure will not abort the export. Notes: a corrupted file might damaged the tar-based file ')
		parser.add_argument('--no-verify', action='store_true', help='Do not verify the exported file contents')
		parser.add_argument('--no-meta', action='store_true', help='Do not add the backup metadata file {!r} in the exported file'.format(constants.BACKUP_META_FILE_NAME))

	@override
	def run(self, args: argparse.Namespace):
		handler = ExportCommandHandler(ExportCommandArgs(
			db_path=Path(args.db),
			backup_id=args.backup_id,
			output_path=Path(args.output),
			format=args.format,
			fail_soft=args.fail_soft,
			no_verify=args.no_verify,
			no_meta=args.no_meta,
		))
		handler.handle()
	