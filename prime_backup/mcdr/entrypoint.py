from mcdreforged.api.all import *

from prime_backup.config.config import Config, set_config_instance
from prime_backup.db.access import DbAccess
from prime_backup.mcdr.commands import CommandManager
from prime_backup.mcdr.task_manager import TaskManager

config: Config
task_manager: TaskManager
command_manager: CommandManager


def on_load(server: PluginServerInterface, old):
	global config, task_manager, command_manager
	config = server.load_config_simple(target_class=Config)
	set_config_instance(config)

	# TODO: respect config.enabled

	DbAccess.init()
	task_manager = TaskManager(server)
	command_manager = CommandManager(server, task_manager)
	command_manager.register_commands()


def on_unload(server: PluginServerInterface):
	pass


def on_info(server: PluginServerInterface, info: Info):
	if not info.is_user:
		for pattern in config.server.saved_world_regex_patterns:
			if pattern.fullmatch(info.content):
				task_manager.on_world_saved()
				break
