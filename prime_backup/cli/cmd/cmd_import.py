import argparse
import dataclasses
import json
import sys
from pathlib import Path
from typing import Optional

from typing_extensions import override

from prime_backup import constants
from prime_backup.action.import_backup_action import BackupMetadataNotFound, ImportBackupAction
from prime_backup.cli import cli_utils
from prime_backup.cli.cmd import CliCommandHandlerBase, CommonCommandArgs, CliCommandAdapterBase
from prime_backup.cli.return_codes import ErrorReturnCodes
from prime_backup.types.standalone_backup_format import StandaloneBackupFormat


@dataclasses.dataclass(frozen=True)
class ImportCommandArgs(CommonCommandArgs):
	input_path: Path
	format: Optional[str]
	auto_meta: bool
	meta_override: Optional[str]


class ImportCommandHandler(CliCommandHandlerBase):
	def __init__(self, args: ImportCommandArgs):
		super().__init__()
		self.args = args

	def handle(self):
		fmt = cli_utils.get_ebf(self.args.input_path, self.args.format)
		self.init_environment(self.args.db_path)

		meta_override: Optional[dict] = None
		if (args_meta_str := self.args.meta_override) is not None:
			try:
				meta_override = json.loads(args_meta_str)
			except ValueError as e:
				self.logger.error('Bad json {!r}: {}'.format(args_meta_str, e))
				sys.exit(1)
			if not isinstance(meta_override, dict):
				self.logger.error('meta_override should be a dict, but found {}: {!r}'.format(type(meta_override), meta_override))
				sys.exit(1)

		self.logger.info('Importing backup from {}, format: {}'.format(str(self.args.input_path.as_posix()), fmt.name))
		try:
			ImportBackupAction(self.args.input_path, fmt, ensure_meta=not self.args.auto_meta, meta_override=meta_override).run()
		except BackupMetadataNotFound as e:
			self.logger.error('Import failed due to backup metadata not found: {}'.format(e))
			self.logger.error('Please make sure the file is a valid backup create by Prime Backup. You can also use the --auto-meta flag for a workaround')
			ErrorReturnCodes.action_failed.sys_exit()


class ImportCommandAdapter(CliCommandAdapterBase):
	@property
	@override
	def command(self) -> str:
		return 'import'

	@property
	@override
	def description(self) -> str:
		return 'Import a backup from the given file. The backup file needs to have a backup metadata file {!r}, or the --auto-meta flag need to be supplied'.format(constants.BACKUP_META_FILE_NAME)

	@override
	def build_parser(self, parser: argparse.ArgumentParser):
		parser.add_argument('input', help='The file name of the backup to be imported. Example: my_backup.tar')
		parser.add_argument('-f', '--format', help='The format of the input file. If not given, attempt to infer from the input file name. Options: {}'.format(cli_utils.enum_options(StandaloneBackupFormat)))
		parser.add_argument('--auto-meta', action='store_true', help='If the backup metadata file does not exist, create an auto-generated one based on the file content')
		parser.add_argument('--meta-override', help='An optional json object string. It overrides the metadata of the imported backup, regardless of whether the backup metadata file exists or not')

	@override
	def run(self, args: argparse.Namespace):
		handler = ImportCommandHandler(ImportCommandArgs(
			db_path=Path(args.db),
			input_path=Path(args.input),
			format=args.format,
			auto_meta=args.auto_meta,
			meta_override=args.meta_override,
		))
		handler.handle()
