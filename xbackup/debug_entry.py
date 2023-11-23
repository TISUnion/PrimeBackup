from pathlib import Path

from xbackup import logger
from xbackup.db import db_logger
from xbackup.db.access import DbAccess
from xbackup.task.back_up_task import BackUpTask
from xbackup.task.export_backup_task import ExportBackupTask, ExportFormat


def main():
	DbAccess.init()
	db_logger.get_logger().addHandler(logger.get().handlers[0])

	bkt = BackUpTask('Steve', 'test')
	bkt.run()
	ExportBackupTask(bkt.backup_id, Path('export.tar.gz'), ExportFormat.tar_gz).run()
	ExportBackupTask(bkt.backup_id, Path('export'), ExportFormat.direct).run()


if __name__ == '__main__':
	main()
