import threading
import time
from typing import Optional

from mcdreforged.api.all import *

from prime_backup.compressors import CompressMethod
from prime_backup.config.config import Config, set_config_instance
from prime_backup.db.access import DbAccess
from prime_backup.mcdr import mcdr_globals
from prime_backup.mcdr.command.commands import CommandManager
from prime_backup.mcdr.crontab_manager import CrontabManager
from prime_backup.mcdr.task_manager import TaskManager
from prime_backup.utils import misc_utils

config: Optional[Config] = None
task_manager: Optional[TaskManager] = None
command_manager: Optional[CommandManager] = None
crontab_manager: Optional[CrontabManager] = None
mcdr_globals.load()
init_ok = False


def __check_config(server: PluginServerInterface):
	if (cm := config.backup.compress_method) == CompressMethod.lzma:
		server.logger.warning('WARN: Using {} as the compress method might significantly increase the backup time'.format(cm.name))


def is_enabled() -> bool:
	return config.enabled


def on_load(server: PluginServerInterface, old):
	global config, task_manager, command_manager, crontab_manager
	try:
		config = server.load_config_simple(target_class=Config, failure_policy='raise')
		set_config_instance(config)
		__check_config(server)
		if not is_enabled():
			return

		DbAccess.init()
		task_manager = TaskManager()
		task_manager.start()
		crontab_manager = CrontabManager(task_manager)
		crontab_manager.start()
		command_manager = CommandManager(server, task_manager, crontab_manager)
		command_manager.register_commands()

		server.register_help_message(config.command.prefix, mcdr_globals.metadata.get_description_rtext())
	except Exception:
		server.logger.error('{} initialization failed and will be disabled'.format(server.get_self_metadata().name))
		on_unload(server)
		raise
	else:
		global init_ok
		init_ok = is_enabled()


def on_unload(server: PluginServerInterface):
	server.logger.info('Shutting down everything...')
	global task_manager, crontab_manager

	def shutdown():
		global task_manager, crontab_manager
		try:
			if command_manager is not None:
				command_manager.close_the_door()
			if crontab_manager is not None:
				crontab_manager.shutdown()
				crontab_manager = None
			if task_manager is not None:
				task_manager.shutdown()
				task_manager = None
			DbAccess.shutdown()
		finally:
			shutdown_event.set()

	shutdown_event = threading.Event()
	thread = threading.Thread(target=shutdown, name=misc_utils.make_thread_name('shutdown'), daemon=True)
	thread.start()

	start_time = time.time()
	for i, delay in enumerate([10, 60, 300, 600, None]):
		elapsed = time.time() - start_time
		if i > 0:
			server.logger.info(f'Waiting for manager shutdown ... time elapsed {elapsed:.1f}s')
			if (cm := crontab_manager) is not None:
				server.logger.info('crontab_manager is still alive')
			elif (tm := task_manager) is not None:
				server.logger.info('task_manager is still alive')
				server.logger.info('task worker heavy: queue size %s current %s', tm.worker_heavy.task_queue.qsize(), tm.worker_heavy.task_queue.current_item)
				server.logger.info('task worker light: queue size %s current %s', tm.worker_light.task_queue.qsize(), tm.worker_heavy.task_queue.current_item)

		shutdown_event.wait(max(0.0, delay - elapsed) if delay is not None else delay)
		if shutdown_event.is_set():
			break
	server.logger.info('Shutdown completes')


def on_info(server: PluginServerInterface, info: Info):
	if init_ok and not info.is_user:
		for pattern in config.server.saved_world_regex:
			if pattern.fullmatch(info.content):
				task_manager.on_world_saved()
				break
