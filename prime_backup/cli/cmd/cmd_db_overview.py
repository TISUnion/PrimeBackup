import argparse
import dataclasses
from pathlib import Path
from typing import Tuple

from typing_extensions import override

from prime_backup.action.get_db_overview_action import GetDbOverviewAction
from prime_backup.cli.cmd import CliCommandHandlerBase, CommonCommandArgs, CliCommandAdapterBase
from prime_backup.types.units import ByteCount


@dataclasses.dataclass(frozen=True)
class DbOverviewCommandArgs(CommonCommandArgs):
	pass


class DbOverviewCommandHandler(CliCommandHandlerBase):
	def __init__(self, args: DbOverviewCommandArgs):
		super().__init__()
		self.args = args

	@staticmethod
	def __size_str(size: int) -> str:
		return '{} ({} bytes)'.format(ByteCount(size).auto_str(), size)

	@staticmethod
	def __ratio_str(numerator: int, denominator: int) -> str:
		if denominator != 0:
			return '{:.2f}%'.format(100 * numerator / denominator)
		return 'N/A'

	def __dedup_stat_str(self, count_deduped: int, count_total: int, size_deduped: int, size_total: int) -> Tuple[str, str]:
		count_saved = count_total - count_deduped
		size_saved = size_total - size_deduped
		count_str = '{} ({} / {})'.format(self.__ratio_str(-count_saved, count_total), count_saved, count_total)
		size_str = '{} ({} / {})'.format(self.__ratio_str(-size_saved, size_total), ByteCount(size_saved).auto_str(), ByteCount(size_total).auto_str())
		return count_str, size_str

	def handle(self):
		self.init_environment(self.args.db_path)
		result = GetDbOverviewAction().run()
		blob_store_stored_size_sum = result.direct_blob_stored_size_sum + result.chunk_stored_size_sum
		blob_store_raw_size_sum = result.direct_blob_raw_size_sum + result.chunk_raw_size_sum

		self.logger.info('======== Database overview ========')
		self.logger.info('Database version: %s', result.db_version)
		self.logger.info('Database file size: %s', self.__size_str(result.db_file_size))
		self.logger.info('Hash method: %s', result.hash_method)

		self.logger.info('[Backup]')
		self.logger.info('Backup count: %s', result.backup_count)
		self.logger.info('Backup data stored size sum: %s (%s)', self.__size_str(blob_store_stored_size_sum), self.__ratio_str(blob_store_stored_size_sum, blob_store_raw_size_sum))
		self.logger.info('Backup data raw size sum: %s', self.__size_str(blob_store_raw_size_sum))

		self.logger.info('[File]')
		self.logger.info('Fileset count: %s', result.fileset_count)
		self.logger.info('File count: %s (%s objects)', result.file_total_count, result.file_object_count)
		self.logger.info('File raw size sum: %s', self.__size_str(result.file_raw_size_sum))

		self.logger.info('[Blob]')
		self.logger.info('Blob count: %s', result.blob_count)
		self.logger.info('Blob stored size sum: %s (%s)', self.__size_str(result.blob_stored_size_sum), self.__ratio_str(result.blob_stored_size_sum, result.blob_raw_size_sum))
		self.logger.info('Blob raw size sum: %s', self.__size_str(result.blob_raw_size_sum))
		blob_count_str, blob_size_str = self.__dedup_stat_str(result.blob_count, result.file_object_count, result.blob_raw_size_sum, result.file_raw_size_sum)
		self.logger.info('Blob dedup stats: count %s, size %s', blob_count_str, blob_size_str)

		self.logger.info('[Chunk]')
		self.logger.info('Chunk count: %s', result.chunk_count)
		self.logger.info('Chunk stored size sum: %s (%s)', self.__size_str(result.chunk_stored_size_sum), self.__ratio_str(result.chunk_stored_size_sum, result.chunk_raw_size_sum))
		self.logger.info('Chunk raw size sum: %s', self.__size_str(result.chunk_raw_size_sum))
		chunk_count_str, chunk_size_str = self.__dedup_stat_str(result.chunk_count, result.chunk_group_chunk_binding_count, result.chunk_raw_size_sum, result.chunked_blob_raw_size_sum)
		self.logger.info('Chunk dedup stats: count %s, size %s', chunk_count_str, chunk_size_str)


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
