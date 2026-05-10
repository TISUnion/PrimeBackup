import argparse
import dataclasses
from pathlib import Path
from typing import Optional

from typing_extensions import override

from prime_backup.action.create_backup_action import CreateBackupAction
from prime_backup.action.export_backup_action_directory import ExportBackupToDirectoryAction
from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.action.list_backup_action import ListBackupAction
from prime_backup.cli import cli_utils
from prime_backup.cli.cmd import CliCommandHandlerBase, CommonCommandArgs, CliCommandAdapterBase
from prime_backup.cli.return_codes import ErrorReturnCodes
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.backup_tags import BackupTags, BackupTagName
from prime_backup.types.operator import Operator, PrimeBackupOperatorNames
from prime_backup.utils import backup_utils
from prime_backup.utils.timer import Timer


@dataclasses.dataclass(frozen=True)
class BackCommandArgs(CommonCommandArgs):
	backup_id: Optional[str]
	source_root: Path
	confirm: bool
	fail_soft: bool
	no_verify: bool
	skip_pre_restore_backup: bool


class BackCommandHandler(CliCommandHandlerBase):
	def __init__(self, args: BackCommandArgs):
		super().__init__()
		self.args = args

	@override
	def requires_user_config_file(self) -> bool:
		return True

	def __get_backup(self) -> BackupInfo:
		if self.args.backup_id is not None:
			backup_id = cli_utils.parse_backup_id(self.args.backup_id)
			return GetBackupAction(backup_id).run()

		backup_filter = BackupFilter()
		backup_filter.requires_non_temporary_backup()
		candidates = ListBackupAction(backup_filter=backup_filter, limit=1).run()
		if len(candidates) == 0:
			self.logger.error('No non-temporary backup found')
			ErrorReturnCodes.backup_not_found.sys_exit()
		return candidates[0]

	def __confirm_restore(self, backup: BackupInfo, source_root: Path):
		if self.args.confirm:
			return

		self.logger.warning('This will restore backup #{} to {!r}'.format(backup.id, source_root.as_posix()))
		self.logger.warning('Existing backup targets will be replaced. Make sure the Minecraft server is stopped before continuing')
		answer = input('Type "yes" to continue: ')
		if answer != 'yes':
			self.logger.info('Restore cancelled')
			ErrorReturnCodes.action_failed.sys_exit()

	def __create_pre_restore_backup(self, backup: BackupInfo, source_root: Path) -> Optional[BackupInfo]:
		if self.args.skip_pre_restore_backup:
			self.logger.info('Pre-restore temporary backup skipped by command line flag')
			return None
		if not self.config.command.backup_on_restore:
			self.logger.info('Pre-restore temporary backup skipped since command.backup_on_restore is false')
			return None
		if not source_root.exists():
			self.logger.info('Pre-restore temporary backup skipped since source root {!r} does not exist'.format(source_root.as_posix()))
			return None

		self.logger.info('Creating temporary backup before restoring to #{} at path {!r}'.format(backup.id, source_root.as_posix()))
		return CreateBackupAction(
			creator=Operator.pb(PrimeBackupOperatorNames.pre_restore),
			comment=backup_utils.create_translated_backup_comment('pre_restore', backup.id),
			tags=BackupTags().set(BackupTagName.temporary, True),
			source_path=source_root,
		).run()

	def handle(self):
		self.init_environment_from_args(self.args)
		source_root = self.args.source_root

		backup = self.__get_backup()
		self.__confirm_restore(backup, source_root)

		timer = Timer()
		pre_restore_backup = self.__create_pre_restore_backup(backup, source_root)
		cost_backup = timer.get_and_restart()
		if pre_restore_backup is not None:
			self.logger.info('Pre-restore temporary backup #{} created, cost {}s'.format(pre_restore_backup.id, round(cost_backup, 2)))

		self.logger.info('Restoring to backup #{} at path {!r} (fail_soft={}, verify_blob={})'.format(
			backup.id, source_root.as_posix(), self.args.fail_soft, not self.args.no_verify,
		))
		failures = ExportBackupToDirectoryAction(
			backup.id, source_root,
			restore_mode=True,
			fail_soft=self.args.fail_soft,
			verify_blob=not self.args.no_verify,
			retain_patterns=self.config.backup.retain_patterns,
		).run()
		cost_restore = timer.get_elapsed()

		if len(failures) > 0:
			self.logger.error('Found {} failures during backup restore'.format(len(failures)))
			for line in failures.to_lines():
				self.logger.error(line.to_plain_text())
			if not self.args.fail_soft:
				ErrorReturnCodes.action_failed.sys_exit()

		self.logger.info('Restore to backup #{} done, cost {}s (backup {}s, restore {}s)'.format(
			backup.id, round(cost_backup + cost_restore, 2), round(cost_backup, 2), round(cost_restore, 2),
		))


class BackCommandAdapter(CliCommandAdapterBase):
	@property
	@override
	def command(self) -> str:
		return 'back'

	@property
	@override
	def description(self) -> str:
		return 'Restore the server files to a backup'

	@override
	def build_parser(self, parser: argparse.ArgumentParser):
		parser.add_argument('backup_id', nargs='?', help='The ID of the backup to restore. Besides an integer ID, it can also be "latest" or latest-offsets like "~", "~3". If omitted, restore the latest non-temporary backup')
		parser.add_argument('-s', '--source-root', required=True, help='The source root directory to restore into')
		parser.add_argument('--confirm', action='store_true', help='Skip the interactive confirmation prompt')
		parser.add_argument('--fail-soft', action='store_true', help='Skip files with restore failure, so a single failure will not abort the restore')
		parser.add_argument('--no-verify', action='store_true', help='Do not verify restored file contents')
		parser.add_argument('--no-pre-restore-backup', '--skip-pre-restore-backup', dest='skip_pre_restore_backup', action='store_true', help='Do not create a temporary backup before restoring')

	@override
	def run(self, args: argparse.Namespace):
		handler = BackCommandHandler(BackCommandArgs(
			db_path=Path(args.db),
			config_path=Path(args.config) if args.config is not None else None,
			backup_id=args.backup_id,
			source_root=Path(args.source_root),
			confirm=args.confirm,
			fail_soft=args.fail_soft,
			no_verify=args.no_verify,
			skip_pre_restore_backup=args.skip_pre_restore_backup,
		))
		handler.handle()
