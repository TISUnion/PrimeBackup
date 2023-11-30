from mcdreforged.api.all import *

from prime_backup.config.config import Config, set_config_instance
from prime_backup.db.access import DbAccess
from prime_backup.mcdr import mcdr_globals
from prime_backup.mcdr.command.commands import CommandManager
from prime_backup.mcdr.task_manager import TaskManager

config: Config
task_manager: TaskManager
command_manager: CommandManager
mcdr_globals.load()
init_ok = False


def on_load(server: PluginServerInterface, old):
	global config, task_manager, command_manager
	config = server.load_config_simple(target_class=Config)
	set_config_instance(config)

	# TODO: respect config.enabled

	DbAccess.init()
	task_manager = TaskManager(server)
	task_manager.start()
	command_manager = CommandManager(server, task_manager)
	command_manager.register_commands()

	server.register_help_message(config.command.prefix, mcdr_globals.metadata.get_description_rtext())

	global init_ok
	init_ok = True


def on_unload(server: PluginServerInterface):
	task_manager.shutdown()
	DbAccess.shutdown()


def on_info(server: PluginServerInterface, info: Info):
	if init_ok and not info.is_user:
		for pattern in config.server.saved_world_regex_patterns:
			if pattern.fullmatch(info.content):
				task_manager.on_world_saved()
				break
