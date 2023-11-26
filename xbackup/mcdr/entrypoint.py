from mcdreforged.api.all import *

from xbackup.config.config import Config, set_config_instance
from xbackup.db.access import DbAccess
from xbackup.mcdr.manager import Manager

config: Config
manager: Manager


def cmd_help(source: CommandSource):
	pass


def cmd_list(source: CommandSource, context: CommandContext):
	pass


def cmd_make(source: CommandSource, context: CommandContext):
	manager.create_backup(source, context.get('comment', ''))


def register_command(server: PluginServerInterface):
	builder = SimpleCommandBuilder()
	builder.command('list', cmd_list)
	builder.command('make', cmd_make)
	builder.command('make <comment>', cmd_make)
	builder.arg('<comment>', GreedyText)

	root = Literal(config.command.prefix).runs(cmd_help)
	builder.add_children_for(root)
	server.register_command(root)


def on_load(server: PluginServerInterface, old):
	global config, manager
	config = server.load_config_simple(target_class=Config)
	set_config_instance(config)

	# TODO: respect config.enabled

	manager = Manager(server)
	DbAccess.init()
	register_command(server)


def on_unload(server: PluginServerInterface):
	pass


def on_info(server: PluginServerInterface, info: Info):
	if not info.is_user:
		for pattern in config.server.saved_world_regex_patterns:
			if pattern.fullmatch(info.content):
				manager.on_world_saved()
				break
