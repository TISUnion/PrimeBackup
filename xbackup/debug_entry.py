import time
from pathlib import Path

from xbackup import logger
from xbackup.db.access import DbAccess
from xbackup.task.action.create_backup_action import CreateBackupAction
from xbackup.task.action.delete_backup_action import DeleteBackupAction
from xbackup.task.action.export_backup_action import ExportBackupActions
from xbackup.task.action.list_backup_action import ListBackupAction
from xbackup.task.types.operator import Operator
from xbackup.task.types.tar_format import TarFormat


# @memory_profiler.profile
def main():
	DbAccess.init()
	# db_logger.get_logger().addHandler(logger.get().handlers[0])

	logger.get().info('start')

	class TA:
		pass
	bkt = TA()
	bkt.backup_id = 1

	def create(n: int = 1):
		nonlocal bkt

		for i in range(n):
			t = time.time()
			bkt = CreateBackupAction(Operator.player('Steve'), '测试彩色测试')
			bkt.run()
			print('cost', round(time.time() - t, 2), 's')

	def create_if_1st():
		nonlocal bkt
		t = time.time()
		if bkt.backup_id == 1:
			bkt = CreateBackupAction(Operator.player('Steve'), 'test2')
			bkt.run()

		print('cost', round(time.time() - t, 2), 's')

	def export():
		t = time.time()

		ExportBackupActions.to_tar(bkt.backup_id, Path('export.tar'), TarFormat.plain).run()
		# ExportBackupTasks.to_tar(bkt.backup_id, Path('export.tar.gz'), TarFormat.gzip).run()
		# ExportBackupTasks.to_tar(bkt.backup_id, Path('export.tar.zst'), TarFormat.zstd).run()
		# ExportBackupTasks.to_tar(bkt.backup_id, Path('export.tar.xz'), TarFormat.lzma).run()
		# ExportBackupTasks.to_zip(bkt.backup_id, Path('export.zip')).run()
		# ExportBackupTasks.to_dir(bkt.backup_id, Path('export'), True).run()

		print('cost', round(time.time() - t, 2), 's')

	def delete():
		t = time.time()
		DeleteBackupAction(bkt.backup_id).run()
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
