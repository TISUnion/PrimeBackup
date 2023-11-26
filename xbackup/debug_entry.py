import time

from xbackup import logger
from xbackup.config.config import Config
from xbackup.db.access import DbAccess
from xbackup.task.create_backup_task import CreateBackupTask
from xbackup.task.delete_backup_task import DeleteBackupTask


# @memory_profiler.profile
def main():
	DbAccess.init()
	# db_logger.get_logger().addHandler(logger.get().handlers[0])

	logger.get().info('start')
	job = Config.get().backup.get_default_job()

	class TA: pass
	bkt = TA()
	bkt.backup_id = 1

	def create(n: int = 1):
		nonlocal bkt

		for i in range(n):
			t = time.time()
			bkt = CreateBackupTask(job, 'Steve', '测试彩色测试')
			bkt.run()
			print('cost', round(time.time() - t, 2), 's')

	def create_if_1st():
		nonlocal bkt
		t = time.time()
		if bkt.backup_id == 1:
			bkt = CreateBackupTask(job, 'Steve', 'test2')
			bkt.run()

		print('cost', round(time.time() - t, 2), 's')

	def export():
		t = time.time()

		# ExportBackupTasks.create_to_tar(bkt.backup_id, Path('export.tar'), TarFormat.plain).run()
		# ExportBackupTasks.create_to_tar(bkt.backup_id, Path('export.tar.gz'), TarFormat.gzip).run()
		# ExportBackupTasks.create_to_tar(bkt.backup_id, Path('export.tar.zst'), TarFormat.zstd).run()
		# ExportBackupTasks.create_to_tar(bkt.backup_id, Path('export.tar.xz'), TarFormat.lzma).run()
		# ExportBackupTasks.create_to_zip(bkt.backup_id, Path('export.zip')).run()
		# ExportBackupTasks.create_to_dir(bkt.backup_id, Path('export')).run()

		print('cost', round(time.time() - t, 2), 's')

	def delete():
		t = time.time()
		DeleteBackupTask(bkt.backup_id).run()
		print('cost', round(time.time() - t, 2), 's')
		t = time.time()

	create(2)
	delete()


if __name__ == '__main__':
	main()
