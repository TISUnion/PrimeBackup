from typing import Optional

from mcdreforged.api.all import *

from prime_backup.compressors import CompressMethod
from prime_backup.config.config import Config, set_config_instance
from prime_backup.db.access import DbAccess
from prime_backup.mcdr import mcdr_globals
from prime_backup.mcdr.command.commands import CommandManager
from prime_backup.mcdr.crontab_manager import CrontabManager
from prime_backup.mcdr.task_manager import TaskManager

config: Optional[Config] = None
task_manager: Optional[TaskManager] = None
command_manager: Optional[CommandManager] = None
crontab_manager: Optional[CrontabManager] = None
mcdr_globals.load()
init_ok = False


def check_config(server: PluginServerInterface):
	if (cm := config.backup.compress_method) == CompressMethod.lzma:
		server.logger.warning('WARN: Using {} as the compress method might significantly increase the backup time'.format(cm.name))


def on_load(server: PluginServerInterface, old):
	global config, task_manager, command_manager, crontab_manager
	try:
		config = server.load_config_simple(target_class=Config)
		set_config_instance(config)
		check_config(server)

		# TODO: respect config.enabled

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
		init_ok = True


def on_unload(server: PluginServerInterface):
	server.logger.info('Shutting down everything...')

	global task_manager, crontab_manager
	if command_manager is not None:
		command_manager.close_the_door()
	if task_manager is not None:
		task_manager.shutdown()
		task_manager = None
	if crontab_manager is not None:
		crontab_manager.shutdown()
		crontab_manager = None
	DbAccess.shutdown()


def on_info(server: PluginServerInterface, info: Info):
	if init_ok and not info.is_user:
		for pattern in config.server.saved_world_regex_patterns:
			if pattern.fullmatch(info.content):
				task_manager.on_world_saved()
				break
