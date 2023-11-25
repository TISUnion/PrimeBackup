import time
import tracemalloc
from pathlib import Path

from xbackup import logger
from xbackup.db.access import DbAccess
from xbackup.task.create_backup_task import CreateBackupTask
from xbackup.task.delete_backup_task import DeleteBackupTask
from xbackup.task.export_backup_task import ExportBackupTasks, TarFormat


# @memory_profiler.profile
def main():
	tracemalloc.start()

	DbAccess.init()
	# db_logger.get_logger().addHandler(logger.get().handlers[0])

	logger.get().info('start')

	t = time.time()
	bkt = CreateBackupTask('Steve', 'test')
	bkt.run()

	print('cost', round(time.time() - t, 2), 's')
	t = time.time()
	if bkt.backup_id == 1:
		bkt = CreateBackupTask('Steve', 'test2')
		bkt.run()

	print('cost', round(time.time() - t, 2), 's')
	t = time.time()

	ExportBackupTasks.create_to_tar(bkt.backup_id, Path('export.tar'), TarFormat.plain).run()

	# ExportBackupTasks.create_to_tar(bkt.backup_id, Path('export.tar.gz'), TarFormat.gzip).run()
	# ExportBackupTasks.create_to_tar(bkt.backup_id, Path('export.tar.zst'), TarFormat.zstd).run()
	# ExportBackupTasks.create_to_tar(bkt.backup_id, Path('export.tar.xz'), TarFormat.lzma).run()
	# ExportBackupTasks.create_to_zip(bkt.backup_id, Path('export.zip')).run()
	# ExportBackupTasks.create_to_dir(bkt.backup_id, Path('export')).run()

	print('cost', round(time.time() - t, 2), 's')
	t = time.time()
	DeleteBackupTask(bkt.backup_id).run()
	print('cost', round(time.time() - t, 2), 's')
	t = time.time()


if __name__ == '__main__':
	main()
