import dataclasses
import logging
import time
from pathlib import Path

from prime_backup import logger
from prime_backup.action.create_backup_action import CreateBackupAction
from prime_backup.action.delete_backup_action import DeleteBackupAction
from prime_backup.action.export_backup_action_directory import ExportBackupToDirectoryAction
from prime_backup.action.export_backup_action_tar import ExportBackupToTarAction
from prime_backup.action.export_backup_action_zip import ExportBackupToZipAction
from prime_backup.action.get_db_overview_action import GetDbOverviewAction
from prime_backup.action.import_backup_action import ImportBackupAction
from prime_backup.action.list_backup_action import ListBackupAction
from prime_backup.action.migrate_compress_method_action import MigrateCompressMethodAction
from prime_backup.action.validate_backups_action import ValidateBackupsAction
from prime_backup.action.validate_blob_chunk_group_bindings_action import ValidateBlobChunkGroupBindingsAction
from prime_backup.action.validate_blobs_action import ValidateBlobsAction
from prime_backup.action.validate_chunk_group_chunk_bindings_action import ValidateChunkGroupChunkBindingsAction
from prime_backup.action.validate_chunk_groups_action import ValidateChunkGroupsAction
from prime_backup.action.validate_chunks_action import ValidateChunksAction
from prime_backup.action.validate_files_action import ValidateFilesAction
from prime_backup.action.validate_filesets_action import ValidateFilesetsAction
from prime_backup.compressors import CompressMethod
from prime_backup.config.config import Config
from prime_backup.db.access import DbAccess
from prime_backup.types.operator import Operator
from prime_backup.types.tar_format import TarFormat


def main():
	logger.get().setLevel(logging.DEBUG)
	Config().get().debug = True

	DbAccess.init(create=True, migrate=True)
	backup_id = 1
	logger.get().info('debug entry start')

	def create(n: int = 1):
		nonlocal backup_id

		for i in range(n):
			t = time.time()
			bka = CreateBackupAction(Operator.player('Steve'), '测试彩色测试')
			backup_id = bka.run().id
			print('cost', round(time.time() - t, 2), 's')

	def create_if_1st():
		nonlocal backup_id
		t = time.time()
		if backup_id == 1:
			bka = CreateBackupAction(Operator.player('Steve'), 'test2')
			backup_id = bka.run().id

		print('cost', round(time.time() - t, 2), 's')

	def export(bid=None):
		if bid is None:
			bid = backup_id
		t = time.time()

		_ = [ExportBackupToDirectoryAction, ExportBackupToTarAction, ExportBackupToZipAction, TarFormat]
		ExportBackupToTarAction(bid, Path('export.tar'), TarFormat.plain).run()
		# ExportBackupToTarAction(bid, Path('export.tar.gz'), TarFormat.gzip).run()
		# ExportBackupToTarAction(bid, Path('export.tar.zst'), TarFormat.zstd).run()
		# ExportBackupToTarAction(bid, Path('export.tar.xz'), TarFormat.lzma).run()
		# ExportBackupToZipAction(bid, Path('export.zip')).run()
		# ExportBackupToDirectoryAction(bid, Path('export')).run()

		print('cost', round(time.time() - t, 2), 's')

	def import_():
		t = time.time()
		bi = ImportBackupAction(Path('export.tar')).run()
		# bi = ImportBackupAction(Path(r'export.tar.zst')).run()
		print('cost', round(time.time() - t, 2), 's')
		nonlocal backup_id
		backup_id = bi.id

	def delete(bid=None):
		if bid is None:
			bid = backup_id
		t = time.time()
		DeleteBackupAction(bid).run()
		print('cost', round(time.time() - t, 2), 's')

	def list_():
		t = time.time()
		for backup in ListBackupAction().run():
			print(backup)
		print('cost', round(time.time() - t, 2), 's')

	def migrate():
		print(MigrateCompressMethodAction(CompressMethod.plain).run())
		print(MigrateCompressMethodAction(CompressMethod.lz4).run())
		print(MigrateCompressMethodAction(CompressMethod.zstd).run())
		print(MigrateCompressMethodAction(CompressMethod.gzip).run())
		# print(MigrateCompressMethodAction(CompressMethod.lzma).run())

	def overview():
		o = GetDbOverviewAction().run()
		print('============= DbOverview =============')
		for k, v in dataclasses.asdict(o).items():
			print(f'{k}: {v}')
		print('============= DbOverview =============')

	def validate():
		validate_results = [
			ValidateBlobsAction().run(),
			ValidateChunkGroupsAction().run(),
			ValidateChunksAction().run(),
			ValidateChunkGroupChunkBindingsAction().run(),
			ValidateBlobChunkGroupBindingsAction().run(),
			ValidateFilesAction().run(),
			ValidateFilesetsAction().run(),
			ValidateBackupsAction().run(),
		]
		all_ok = True
		for result in validate_results:
			print(result)
			all_ok &= result.bad == 0
		print(f'Validation summary: {"OK" if all_ok else "FAILED"}')

	create(1)
	delete()
	# export()
	# import_()
	# list_()
	# delete()
	# migrate()
	validate()
	overview()


if __name__ == '__main__':
	main()
