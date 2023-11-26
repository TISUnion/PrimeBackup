import time

from xbackup import logger
from xbackup.db.access import DbAccess
from xbackup.task.create_backup_task import CreateBackupTask
from xbackup.task.delete_backup_task import DeleteBackupTask
from xbackup.task.list_backup_task import ListBackupTask
from xbackup.types import Operator


# @memory_profiler.profile
def main():
	DbAccess.init()
	# db_logger.get_logger().addHandler(logger.get().handlers[0])

	logger.get().info('start')

	class TA: pass
	bkt = TA()
	bkt.backup_id = 1

	def create(n: int = 1):
		nonlocal bkt

		for i in range(n):
			t = time.time()
			bkt = CreateBackupTask(Operator.player('Steve'), '测试彩色测试')
			bkt.run()
			print('cost', round(time.time() - t, 2), 's')

	def create_if_1st():
		nonlocal bkt
		t = time.time()
		if bkt.backup_id == 1:
			bkt = CreateBackupTask(Operator.player('Steve'), 'test2')
			bkt.run()

		print('cost', round(time.time() - t, 2), 's')

	def export():
		t = time.time()

		# ExportBackupTasks.to_tar(bkt.backup_id, Path('export.tar'), TarFormat.plain).run()
		# ExportBackupTasks.to_tar(bkt.backup_id, Path('export.tar.gz'), TarFormat.gzip).run()
		# ExportBackupTasks.to_tar(bkt.backup_id, Path('export.tar.zst'), TarFormat.zstd).run()
		# ExportBackupTasks.to_tar(bkt.backup_id, Path('export.tar.xz'), TarFormat.lzma).run()
		# ExportBackupTasks.to_zip(bkt.backup_id, Path('export.zip')).run()
		# ExportBackupTasks.to_dir(bkt.backup_id, Path('export'), True).run()

		print('cost', round(time.time() - t, 2), 's')

	def delete():
		t = time.time()
		DeleteBackupTask(bkt.backup_id).run()
		print('cost', round(time.time() - t, 2), 's')
		t = time.time()

	def lst():
		for backup in ListBackupTask().run():
			print(backup)

	create(1)
	lst()
	export()
	delete()


if __name__ == '__main__':
	main()
