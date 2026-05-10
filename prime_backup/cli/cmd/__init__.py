import argparse
import dataclasses
import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

from prime_backup import logger
from prime_backup.cli.return_codes import ErrorReturnCodes
from prime_backup.config.config import Config, set_config_instance
from prime_backup.db import db_constants
from prime_backup.db.access import DbAccess
from prime_backup.db.migration import BadDbVersion
from prime_backup.utils.backup_id_parser import BackupIdParser


@dataclasses.dataclass(frozen=True)
class CommonCommandArgs:
	db_path: Path
	config_path: Optional[Path]


class CliCommandHandlerBase(ABC):
	def __init__(self):
		self.logger: logging.Logger = logger.get()

	@property
	def config(self) -> Config:
		return Config.get()

	# ==================== Utils ====================

	def init_environment_from_args(self, args: CommonCommandArgs):
		self.init_environment(args.db_path, config_path=args.config_path)

	def init_environment(self, db_path: Path, *, migrate: bool = False, config_path: Optional[Path] = None):
		root_path = db_path
		if root_path.is_file() and root_path.name == db_constants.DB_FILE_NAME:
			root_path = root_path.parent

		config = self.load_config(root_path, config_path)
		set_config_instance(config)

		if not (dbf := root_path / db_constants.DB_FILE_NAME).is_file():
			self.logger.error('Database file {!r} does not exist'.format(dbf.as_posix()))
			ErrorReturnCodes.invalid_argument.sys_exit()
		self.init_db(root_path, create=False, migrate=migrate)

	@classmethod
	def load_config(cls, root_path: Path, config_path: Optional[Path]) -> Config:
		if config_path is None:
			auto_config_path = root_path.parent / 'config' / 'prime_backup' / 'config.json'
			if auto_config_path.is_file():
				config = cls.__load_config(auto_config_path)
				logger.get().info('Config file auto-detected and loaded from {!r}'.format(auto_config_path.as_posix()))
			else:
				config = Config.get_default()
				logger.get().info('Config file not provided; auto-detected path {!r} does not exist, using default config'.format(auto_config_path.as_posix()))
		else:
			config = cls.__load_config(config_path)
			logger.get().info('Config file loaded from {!r}'.format(config_path.as_posix()))
		return config

	def init_db(self, root_path: Path, *, create: bool, migrate: bool):
		if root_path.is_file():
			self.logger.error('Storage root {!r} is a file'.format(root_path.as_posix()))
			ErrorReturnCodes.invalid_argument.sys_exit()

		config = Config.get()
		config.storage_root = str(root_path.as_posix())

		self.logger.info('Storage root set to {!r}'.format(config.storage_root))
		try:
			if DbAccess.is_initialized():
				DbAccess.shutdown()
			DbAccess.init(create=create, migrate=migrate)
		except BadDbVersion as e:
			self.logger.info('Load database failed, you need to ensure the database is accessible with MCDR plugin: {}'.format(e))
			ErrorReturnCodes.action_failed.sys_exit()
		config.backup.hash_method = DbAccess.get_hash_method()  # use the hash method from the db

	@classmethod
	def __load_config(cls, config_path: Path) -> Config:
		try:
			with config_path.open('r', encoding='utf8') as f:
				return Config.deserialize(json.load(f))
		except Exception as e:
			logger.get().error('Failed to load config file {!r}: {}'.format(config_path.as_posix(), e))
			ErrorReturnCodes.invalid_argument.sys_exit()


class CliCommandAdapterBase(ABC):
	@property
	@abstractmethod
	def command(self) -> str:
		raise NotImplementedError()

	@property
	@abstractmethod
	def description(self) -> str:
		raise NotImplementedError()

	@abstractmethod
	def build_parser(self, parser: argparse.ArgumentParser):
		raise NotImplementedError()

	@abstractmethod
	def run(self, args: argparse.Namespace):
		raise NotImplementedError()

	# ==================== Utils ====================

	@classmethod
	def _add_pos_argument_backup_id(cls, parser: argparse.ArgumentParser):
		def backup_id(s: str):
			_ = BackupIdParser(allow_db_access=True, dry_run=True).parse(s)  # raises ValueError if it's invalid
			return s

		parser.add_argument('backup_id', type=backup_id, help='The ID of the backup to export. Besides an integer ID, it can also be "latest" or latest-offsets like "~", "~3"')
