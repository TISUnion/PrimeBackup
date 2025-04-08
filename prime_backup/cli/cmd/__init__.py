import argparse
import contextlib
import dataclasses
import logging
import sys
from abc import ABC, abstractmethod
from pathlib import Path

from prime_backup import logger
from prime_backup.cli.cli_utils import BackupIdAlternatives
from prime_backup.config.config import Config, set_config_instance
from prime_backup.db import db_constants
from prime_backup.db.access import DbAccess
from prime_backup.db.migration import BadDbVersion


@dataclasses.dataclass(frozen=True)
class CommonCommandArgs:
	db_path: Path


class CliCommandHandlerBase(ABC):
	def __init__(self):
		self.logger: logging.Logger = logger.get()

	@property
	def config(self) -> Config:
		return Config.get()

	# ==================== Utils ====================

	def init_environment(self, db_path: Path, *, migrate: bool = False):
		config = Config.get_default()
		set_config_instance(config)

		root_path = db_path
		if root_path.is_file() and root_path.name == db_constants.DB_FILE_NAME:
			root_path = root_path.parent

		if not (dbf := root_path / db_constants.DB_FILE_NAME).is_file():
			self.logger.error('Database file {!r} does not exist'.format(dbf.as_posix()))
			sys.exit(1)
		config.storage_root = str(root_path.as_posix())

		self.logger.info('Storage root set to {!r}'.format(config.storage_root))
		try:
			DbAccess.init(create=False, migrate=migrate)
		except BadDbVersion as e:
			self.logger.info('Load database failed, you need to ensure the database is accessible with MCDR plugin: {}'.format(e))
			sys.exit(1)
		config.backup.hash_method = DbAccess.get_hash_method()  # use the hash method from the db


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
			with contextlib.suppress(ValueError):
				return int(s)
			with contextlib.suppress(KeyError):
				_ = BackupIdAlternatives[s.lower()]
				return s
			raise ValueError()

		parser.add_argument('backup_id', type=backup_id, help='The ID of the backup to export. Besides an integer ID, it can also be "latest" and "latest_non_temp"')
