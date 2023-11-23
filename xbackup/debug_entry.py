from xbackup import logger
from xbackup.db_access import DbAccess
from xbackup.task.back_up_task import BackUpTask


def main():
	DbAccess.init()
	DbAccess.get_logger().addHandler(logger.get().handlers[0])
	BackUpTask('test').run()


if __name__ == '__main__':
	main()
