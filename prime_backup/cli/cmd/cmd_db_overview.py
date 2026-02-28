import argparse
import dataclasses
from pathlib import Path

from typing_extensions import override

from prime_backup.action.get_db_overview_action import GetDbOverviewAction
from prime_backup.cli.cmd import CliCommandHandlerBase, CommonCommandArgs, CliCommandAdapterBase
from prime_backup.db.access import DbAccess
from prime_backup.types.units import ByteCount


@dataclasses.dataclass(frozen=True)
class DbOverviewCommandArgs(CommonCommandArgs):
	pass


class DbOverviewCommandHandler(CliCommandHandlerBase):
	def __init__(self, args: DbOverviewCommandArgs):
		super().__init__()
		self.args = args

	def handle(self):
		self.init_environment(self.args.db_path)
		result = GetDbOverviewAction().run()
		blob_store_stored_size_sum = result.direct_blob_stored_size_sum + result.chunk_stored_size_sum
		blob_store_raw_size_sum = result.direct_blob_raw_size_sum + result.chunk_raw_size_sum

		self.logger.info('[Basic]')
		self.logger.info('DB version: %s', result.db_version)
		self.logger.info('DB path: %s', DbAccess.get_db_file_path())
		self.logger.info('DB file size: %s (%s)', result.db_file_size, ByteCount(result.db_file_size).auto_str())
		self.logger.info('Hash method: %s', result.hash_method)
		self.logger.info('[Backup]')
		self.logger.info('Backup count: %s', result.backup_count)
		self.logger.info('Backup data stored size sum: %s (%s)', blob_store_stored_size_sum, ByteCount(blob_store_stored_size_sum).auto_str())
		self.logger.info('Backup data raw size sum: %s (%s)', blob_store_raw_size_sum, ByteCount(blob_store_raw_size_sum).auto_str())
		self.logger.info('[File]')
		self.logger.info('Fileset count: %s', result.fileset_count)
		self.logger.info('File count: %s (%s objects)', result.file_total_count, result.file_object_count)
		self.logger.info('File raw size sum: %s (%s)', result.file_raw_size_sum, ByteCount(result.file_raw_size_sum).auto_str())
		self.logger.info('[Blob]')
		self.logger.info('Blob count: %s', result.blob_count)
		self.logger.info('Blob stored size sum: %s (%s)', result.blob_stored_size_sum, ByteCount(result.blob_stored_size_sum).auto_str())
		self.logger.info('Blob raw size sum: %s (%s)', result.blob_raw_size_sum, ByteCount(result.blob_raw_size_sum).auto_str())
		self.logger.info('[Chunk]')
		self.logger.info('Chunk count: %s', result.chunk_count)
		self.logger.info('Chunk stored size sum: %s (%s)', result.chunk_stored_size_sum, ByteCount(result.chunk_stored_size_sum).auto_str())
		self.logger.info('Chunk raw size sum: %s (%s)', result.chunk_raw_size_sum, ByteCount(result.chunk_raw_size_sum).auto_str())


class DbOverviewCommandAdapter(CliCommandAdapterBase):
	@property
	@override
	def command(self) -> str:
		return 'overview'

	@property
	@override
	def description(self) -> str:
		return 'Show overview information of the database'

	@override
	def build_parser(self, parser: argparse.ArgumentParser):
		pass

	@override
	def run(self, args: argparse.Namespace):
		handler = DbOverviewCommandHandler(DbOverviewCommandArgs(
			db_path=Path(args.db),
		))
		handler.handle()
