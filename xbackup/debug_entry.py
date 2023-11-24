from pathlib import Path

from xbackup import logger
from xbackup.db import db_logger
from xbackup.db.access import DbAccess
from xbackup.task.back_up_task import BackUpTask
from xbackup.task.delete_backup_task import DeleteBackupTask
from xbackup.task.export_backup_task import TarFormat, ExportBackupTasks


def main():
	DbAccess.init()
	db_logger.get_logger().addHandler(logger.get().handlers[0])

	bkt = BackUpTask('Steve', 'test')
	bkt.run()
	ExportBackupTasks.create_to_tar(bkt.backup_id, Path('export.tar.gz'), TarFormat.gzip).run()
	ExportBackupTasks.create_to_tar(bkt.backup_id, Path('export.tar'), TarFormat.plain).run()
	ExportBackupTasks.create_to_dir(bkt.backup_id, Path('export')).run()
	ExportBackupTasks.create_to_zip(bkt.backup_id, Path('export.zip')).run()

	DeleteBackupTask(bkt.backup_id).run()


if __name__ == '__main__':
	main()
