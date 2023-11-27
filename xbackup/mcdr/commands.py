from typing import List

from mcdreforged.api.all import *

from xbackup.config.config import Config
from xbackup.mcdr.task_manager import TaskManager
from xbackup.types.backup_filter import BackupFilter
from xbackup.utils.mcdr_utils import tr


class CommandManager:
	def __init__(self, server: PluginServerInterface, task_manager: TaskManager):
		self.server = server
		self.task_manager = task_manager
		self.config = Config.get()

	def cmd_help(self, source: CommandSource):
		pass

	def cmd_list(self, source: CommandSource, context: CommandContext):
		limit = context.get('limit', 10)
		backup_filter = BackupFilter()
		self.task_manager.list_backup(source, limit, backup_filter)

	def cmd_make(self, source: CommandSource, context: CommandContext):
		self.task_manager.create_backup(source, context.get('comment', ''))

	def cmd_delete(self, source: CommandSource, context: CommandContext):
		self.task_manager.delete_backup(source, context['backup_id'])

	def cmd_back(self, source: CommandSource, context: CommandContext):
		self.task_manager.restore_backup(source, context['backup_id'])

	def cmd_confirm(self, source: CommandSource, context: CommandContext):
		if not self.task_manager.do_confirm():
			source.reply(tr('command.confirm.noop'))

	def cmd_cancel(self, source: CommandSource, context: CommandContext):
		if not self.task_manager.do_cancel():
			source.reply(tr('command.cancel.noop'))

	def cmd_abort(self, source: CommandSource, context: CommandContext):
		if not self.task_manager.do_abort():
			source.reply(tr('command.abort.noop'))

	def suggest_backup_id(self) -> List[str]:
		return []  # TODO

	def register_commands(self):
		builder = SimpleCommandBuilder()

		builder.command('list', self.cmd_list)
		builder.command('list <limit>', self.cmd_list)
		builder.command('make', self.cmd_make)
		builder.command('make <comment>', self.cmd_make)
		builder.command('del <backup_id>', self.cmd_delete)
		builder.command('delete <backup_id>', self.cmd_delete)
		builder.command('back', self.cmd_back)
		builder.command('back <backup_id>', self.cmd_back)
		builder.command('confirm', self.cmd_confirm)
		builder.command('cancel', self.cmd_cancel)
		builder.command('abort', self.cmd_abort)

		builder.arg('limit', lambda n: Integer(n).at_min(1))
		builder.arg('comment', GreedyText)
		builder.arg('backup_id', Integer).suggests(self.suggest_backup_id)

		root = Literal(self.config.command.prefix).runs(self.cmd_help)
		builder.add_children_for(root)

		self.server.register_command(root)
