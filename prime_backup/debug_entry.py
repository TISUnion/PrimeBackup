import time
from pathlib import Path

from prime_backup import logger
from prime_backup.action.create_backup_action import CreateBackupAction
from prime_backup.action.delete_backup_action import DeleteBackupAction
from prime_backup.action.export_backup_action import ExportBackupToDirectoryAction, ExportBackupToTarAction, ExportBackupToZipAction
from prime_backup.action.import_backup_action import ImportBackupAction
from prime_backup.action.list_backup_action import ListBackupAction
from prime_backup.db.access import DbAccess
from prime_backup.types.operator import Operator
from prime_backup.types.tar_format import TarFormat


# @memory_profiler.profile
def main():
	DbAccess.init()
	# db_logger.get_logger().addHandler(logger.get().handlers[0])

	logger.get().info('start')

	backup_id = 1

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

	def export():
		t = time.time()

		_ = [ExportBackupToDirectoryAction, ExportBackupToTarAction, ExportBackupToZipAction, TarFormat]
		# ExportBackupToTarAction(backup_id, Path('export.tar'), TarFormat.plain).run()
		# ExportBackupToTarAction(backup_id, Path('export.tar.gz'), TarFormat.gzip).run()
		# ExportBackupToTarAction(backup_id, Path('export.tar.zst'), TarFormat.zstd).run()
		# ExportBackupToTarAction(backup_id, Path('export.tar.xz'), TarFormat.lzma).run()
		# ExportBackupToZipAction(backup_id, Path('export.zip')).run()
		ExportBackupToDirectoryAction(backup_id, Path('export'), delete_existing=True).run()

		print('cost', round(time.time() - t, 2), 's')

	def import_():
		t = time.time()
		# bi = ImportBackupAction(Path('export.tar')).run()
		bi = ImportBackupAction(Path('export.zip')).run()
		print('cost', round(time.time() - t, 2), 's')
		nonlocal backup_id
		backup_id = bi.id

	def delete():
		t = time.time()
		DeleteBackupAction(backup_id).run()
		print('cost', round(time.time() - t, 2), 's')

	def list_():
		t = time.time()
		for backup in ListBackupAction().run():
			print(backup)
		print('cost', round(time.time() - t, 2), 's')

	create(2)
	# import_()
	# list_()
	# export()
	# delete()


if __name__ == '__main__':
	main()
