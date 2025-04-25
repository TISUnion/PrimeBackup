import contextlib
import functools
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
from prime_backup.mcdr.online_player_counter import OnlinePlayerCounter
from prime_backup.mcdr.task_manager import TaskManager
from prime_backup.utils import misc_utils

config: Optional[Config] = None
task_manager: Optional[TaskManager] = None
command_manager: Optional[CommandManager] = None
crontab_manager: Optional[CrontabManager] = None
online_player_counter: Optional[OnlinePlayerCounter] = None
mcdr_globals.load()
init_ok: Optional[bool] = None  # False: failed, True: succeeded, None: not done yet
init_thread: Optional[threading.Thread] = None


def __check_config(server: PluginServerInterface):
	db_hash_method = DbAccess.get_hash_method()
	db_hash_str = db_hash_method.name
	cfg_hash_str = config.backup.hash_method.name
	if cfg_hash_str != db_hash_str:
		server.logger.warning('WARN: Hash method mismatched! config: {}, database: {}. Use the database one'.format(cfg_hash_str, db_hash_str))
	db_hash_method.value.create_hasher()  # ensure lib exists

	if (cm := config.backup.compress_method) == CompressMethod.lzma:
		server.logger.warning('WARN: Using {} as the compress method might significantly increase the backup time'.format(cm.name))
	cm.value.ensure_lib()


def is_enabled() -> bool:
	return config.enabled


def on_load(server: PluginServerInterface, old):
	@contextlib.contextmanager
	def handle_init_error():
		try:
			yield
		except Exception:
			server.logger.error('{} initialization failed and will be disabled'.format(server.get_self_metadata().name))
			server.schedule_task(functools.partial(on_unload, server))
			raise

	def init():
		"""
		The init progress might be costly, don't block the task executor thread
		"""
		with handle_init_error():
			DbAccess.init(create=True, migrate=True)
			__check_config(server)

			task_manager.start()
			crontab_manager.start()
			command_manager.construct_command_tree()

		global init_ok
		init_ok = is_enabled()

	global config, task_manager, command_manager, crontab_manager, online_player_counter
	with handle_init_error():
		config = server.load_config_simple(target_class=Config, failure_policy='raise')
		set_config_instance(config)
		if not is_enabled():
			server.logger.warning('{} is disabled by config'.format(mcdr_globals.metadata.name))
			return

		task_manager = TaskManager()
		crontab_manager = CrontabManager(task_manager)
		command_manager = CommandManager(server, task_manager, crontab_manager)
		online_player_counter = OnlinePlayerCounter(server)

		# registrations need to be done in the on_load() function
		command_manager.register_command_node()
		server.register_help_message(config.command.prefix, mcdr_globals.metadata.get_description_rtext())

		# OnlinePlayerCounter does not need DbAccess,
		# and it needs to initialized before server-start to detect the server-start event
		online_player_counter.on_load(getattr(old, 'online_player_counter', None))

		global init_thread
		init_thread = threading.Thread(target=init, name=misc_utils.make_thread_name('init'), daemon=True)
		init_thread.start()


_has_unload = False
_has_unload_lock = threading.Lock()


def on_unload(server: PluginServerInterface):
	with _has_unload_lock:
		global _has_unload
		if _has_unload:
			return
		_has_unload = True

	server.logger.info('Shutting down everything...')
	global task_manager, crontab_manager

	def shutdown():
		global task_manager, crontab_manager
		try:
			if init_thread is not None:
				init_thread.join()
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
			if init_thread is not None and init_thread.is_alive():
				server.logger.info('init_thread is still running')
			elif (cm := crontab_manager) is not None:
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


def on_server_start(server: PluginServerInterface):
	if init_ok is not False and online_player_counter is not None:
		online_player_counter.on_server_start()


def on_server_stop(server: PluginServerInterface, server_return_code: int):
	if init_ok:
		task_manager.on_server_stopped()


def on_player_joined(server: PluginServerInterface, player: str, info: Info):
	if init_ok is not False and online_player_counter is not None:
		online_player_counter.on_player_joined(player)


def on_player_left(_: PluginServerInterface, player: str):
	if init_ok is not False and online_player_counter is not None:
		online_player_counter.on_player_left(player)
