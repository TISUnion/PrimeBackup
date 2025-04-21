import argparse
import dataclasses
import os
import shlex
from pathlib import Path

from typing_extensions import override

from prime_backup import constants
from prime_backup.cli.cmd import CliCommandHandlerBase, CommonCommandArgs, CliCommandAdapterBase
from prime_backup.cli.return_codes import ErrorReturnCodes


@dataclasses.dataclass(frozen=True)
class FuseCommandArgs(CommonCommandArgs):
	mount_point: str
	foreground: bool
	debug: bool
	allow_other: bool
	no_cache: bool
	no_meta: bool


class FuseCommandHandler(CliCommandHandlerBase):
	def __init__(self, args: FuseCommandArgs):
		super().__init__()
		self.args = args

	def __adjust_fuse_config(self):
		from prime_backup.cli.fuse.config import FuseConfig
		fuse_config = FuseConfig.get()

		fuse_config.no_cache = self.args.no_cache
		fuse_config.no_meta = self.args.no_meta
		if self.args.debug:
			fuse_config.log_call = True

	def handle(self):
		try:
			import fuse
		except ImportError:
			self.logger.error('fuse package not found')
			ErrorReturnCodes.missing_dependency.sys_exit()

		self.__adjust_fuse_config()
		self.init_environment(self.args.db_path)

		from prime_backup.cli.fuse.fs import PrimeBackupFuseFs
		fs = PrimeBackupFuseFs()

		fs.multithreaded = False
		fs.fuse_args.mountpoint = self.args.mount_point
		if self.args.foreground:
			fs.fuse_args.setmod('foreground')
		if self.args.allow_other:
			fs.fuse_args.add('allow_other')
		if self.args.debug:
			fs.fuse_args.add('debug')

		self.logger.info('PID: {}'.format(os.getpid()))
		if self.args.debug or self.args.foreground:
			self.logger.info('Starting fuse at {!r} in foreground...'.format(self.args.mount_point))
		else:
			self.logger.info('Starting fuse at {!r} in background...'.format(self.args.mount_point))
		self.logger.info('Tips: Use command \'{}\' to unmount'.format(shlex.join(['sudo', 'fusermount', '-zu', os.path.abspath(self.args.mount_point)])))

		fs.main()


class FuseCommandAdapter(CliCommandAdapterBase):
	@property
	@override
	def command(self) -> str:
		return 'fuse'

	@property
	@override
	def description(self) -> str:
		return 'Run fuse'

	@override
	def build_parser(self, parser: argparse.ArgumentParser):
		parser.add_argument('mount', help='Path to the fuse mount point')
		parser.add_argument('-f', '--foreground', action='store_true', help='Run in foreground')
		parser.add_argument('--allow-other', action='store_true', help='Allow other users to access the fuse filesystem')
		parser.add_argument('--debug', action='store_true', help='Enable fuse debug and more debug logging')
		parser.add_argument('--no-cache', action='store_true', help='Disabled database access cache')
		parser.add_argument('--no-meta', action='store_true', help='Do not add the backup metadata file {!r} in the exported file'.format(constants.BACKUP_META_FILE_NAME))

	@override
	def run(self, args: argparse.Namespace):
		handler = FuseCommandHandler(FuseCommandArgs(
			db_path=Path(args.db),
			mount_point=args.mount,
			foreground=args.foreground,
			debug=args.debug,
			allow_other=args.allow_other,
			no_cache=args.no_cache,
			no_meta=args.no_meta,
		))
		handler.handle()
