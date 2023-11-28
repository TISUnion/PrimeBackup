from typing import List

from mcdreforged.api.all import *

from prime_backup.config.config import Config
from prime_backup.mcdr.task.create_backup_task import CreateBackupTask
from prime_backup.mcdr.task.delete_backup_task import DeleteBackupTask
from prime_backup.mcdr.task.list_backup_task import ListBackupTask
from prime_backup.mcdr.task.restore_backup_task import RestoreBackupTask
from prime_backup.mcdr.task_manager import TaskManager
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.utils.mcdr_utils import tr


class CommandManager:
	def __init__(self, server: PluginServerInterface, task_manager: TaskManager):
		self.server = server
		self.task_manager = task_manager
		self.config = Config.get()

	def cmd_help(self, source: CommandSource):
		pass

	def __cmd_list(self, source: CommandSource, context: CommandContext, show_hidden: bool):
		limit = context.get('limit', 10)
		page = context.get('page', 1)
		backup_filter = BackupFilter()
		if not show_hidden:
			backup_filter.hidden = False
		self.task_manager.add_read_task(source, tr('task.list'), ListBackupTask(source, limit, page, backup_filter))

	def cmd_list(self, source: CommandSource, context: CommandContext):
		return self.__cmd_list(source, context, False)

	def cmd_list_all(self, source: CommandSource, context: CommandContext):
		return self.__cmd_list(source, context, True)

	def cmd_make(self, source: CommandSource, context: CommandContext):
		comment = context['comment']
		self.task_manager.add_operate_task(source, tr('task.create'), CreateBackupTask(source, comment))

	def cmd_delete(self, source: CommandSource, context: CommandContext):
		backup_id = context['backup_id']
		self.task_manager.add_operate_task(source, tr('task.delete'), DeleteBackupTask(source, backup_id))

	def cmd_back(self, source: CommandSource, context: CommandContext):
		backup_id = context['backup_id']
		self.task_manager.add_operate_task(source, tr('task.restore'), RestoreBackupTask(source, backup_id))

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
		builder.command('list_all', self.cmd_list_all)
		builder.command('list_all <limit>', self.cmd_list_all)
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
