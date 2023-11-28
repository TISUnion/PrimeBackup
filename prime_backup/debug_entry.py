import time
from pathlib import Path

from prime_backup import logger
from prime_backup.action.create_backup_action import CreateBackupAction
from prime_backup.action.delete_backup_action import DeleteBackupAction
from prime_backup.action.export_backup_action import ExportBackupActions
from prime_backup.action.list_backup_action import ListBackupAction
from prime_backup.db.access import DbAccess
from prime_backup.types.operator import Operator
from prime_backup.types.tar_format import TarFormat


# @memory_profiler.profile
def main():
	DbAccess.init()
	# db_logger.get_logger().addHandler(logger.get().handlers[0])

	logger.get().info('start')

	class TA:
		pass
	bka = TA()
	bka.backup_id = 1

	def create(n: int = 1):
		nonlocal bka

		for i in range(n):
			t = time.time()
			bka = CreateBackupAction(Operator.player('Steve'), '测试彩色测试')
			bka.run()
			print('cost', round(time.time() - t, 2), 's')

	def create_if_1st():
		nonlocal bka
		t = time.time()
		if bka.backup_id == 1:
			bka = CreateBackupAction(Operator.player('Steve'), 'test2')
			bka.run()

		print('cost', round(time.time() - t, 2), 's')

	def export():
		t = time.time()

		ExportBackupActions.to_tar(bka.backup_id, Path('export.tar'), TarFormat.plain).run()
		# ExportBackupTasks.to_tar(bka.backup_id, Path('export.tar.gz'), TarFormat.gzip).run()
		# ExportBackupTasks.to_tar(bka.backup_id, Path('export.tar.zst'), TarFormat.zstd).run()
		# ExportBackupTasks.to_tar(bka.backup_id, Path('export.tar.xz'), TarFormat.lzma).run()
		# ExportBackupTasks.to_zip(bka.backup_id, Path('export.zip')).run()
		# ExportBackupTasks.to_dir(bka.backup_id, Path('export'), True).run()

		print('cost', round(time.time() - t, 2), 's')

	def delete():
		t = time.time()
		DeleteBackupAction(bka.backup_id).run()
		print('cost', round(time.time() - t, 2), 's')
		t = time.time()

	def lst():
		for backup in ListBackupAction().run():
			print(backup)

	create(1)
	# lst()
	# export()
	# delete()


if __name__ == '__main__':
	main()
