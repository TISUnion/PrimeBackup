import functools
from typing import List, Callable

from mcdreforged.api.all import *

from prime_backup.config.config import Config
from prime_backup.mcdr.command.nodes import DateNode, IdRangeNode
from prime_backup.mcdr.crontab_job import CrontabJobEvent, CrontabJobId
from prime_backup.mcdr.crontab_manager import CrontabManager
from prime_backup.mcdr.task.backup.create_backup_task import CreateBackupTask
from prime_backup.mcdr.task.backup.delete_backup_range_task import DeleteBackupRangeTask
from prime_backup.mcdr.task.backup.delete_backup_task import DeleteBackupTask
from prime_backup.mcdr.task.backup.export_backup_task import ExportBackupTask, ExportBackupFormat
from prime_backup.mcdr.task.backup.list_backup_task import ListBackupTask
from prime_backup.mcdr.task.backup.prune_backup_task import PruneAllBackupTask
from prime_backup.mcdr.task.backup.rename_backup_task import RenameBackupTask
from prime_backup.mcdr.task.backup.restore_backup_task import RestoreBackupTask
from prime_backup.mcdr.task.backup.show_backup_task import ShowBackupTask
from prime_backup.mcdr.task.crontab.operate_crontab_task import OperateCrontabJobTask
from prime_backup.mcdr.task.crontab.show_crontab_task import ShowCrontabJobTask
from prime_backup.mcdr.task.general.show_help_task import ShowHelpTask
from prime_backup.mcdr.task_manager import TaskManager
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.types.operator import Operator
from prime_backup.utils.mcdr_utils import tr, reply_message, mkcmd


class CommandManager:
	COMMANDS_WITH_DETAILED_HELP = ['list']

	def __init__(self, server: PluginServerInterface, task_manager: TaskManager, crontab_manager: CrontabManager):
		self.server = server
		self.task_manager = task_manager
		self.crontab_manager = crontab_manager
		self.config = Config.get()
		self.plugin_disabled = False

	def close_the_door(self):
		self.plugin_disabled = True

	def cmd_welcome(self, source: CommandSource, context: CommandContext):
		# TODO
		self.cmd_help(source, context, full=True)

	def cmd_help(self, source: CommandSource, context: CommandContext, *, full: bool = False):
		what = context.get('what')
		if what is not None and what not in self.COMMANDS_WITH_DETAILED_HELP:
			reply_message(source, tr('command.help.no_help', RText(mkcmd(what), RColor.gray)))
			return

		self.task_manager.add_task(ShowHelpTask(source, full, what))

	def cmd_make(self, source: CommandSource, context: CommandContext):
		def callback(err):
			if err is None:
				self.crontab_manager.send_event(CrontabJobEvent.manual_backup_created)

		comment = context.get('comment', '')
		self.task_manager.add_task(CreateBackupTask(source, comment), callback)

	def cmd_back(self, source: CommandSource, context: CommandContext, *, skip_confirm: bool = False):
		backup_id = context.get('backup_id')
		self.task_manager.add_task(RestoreBackupTask(source, backup_id, skip_confirm=skip_confirm))

	def cmd_list(self, source: CommandSource, context: CommandContext):
		page = context.get('page', 1)
		per_page = context.get('per_page', 10)

		backup_filter = BackupFilter()
		if (start_date := context.get('start_date')) is not None:
			backup_filter.timestamp_start = int(start_date)
		if (end_date := context.get('end_date')) is not None:
			backup_filter.timestamp_end = int(end_date)
		if (author_str := context.get('author')) is not None:
			if ':' in author_str:
				author = Operator.of(author_str)
			else:
				author = Operator.player(author_str)
			backup_filter.author = author
		show_all = context.get('all', 0) > 0
		show_size = context.get('size', 0) > 0

		self.task_manager.add_task(ListBackupTask(source, per_page, page, backup_filter, show_all, show_size))

	def cmd_show(self, source: CommandSource, context: CommandContext):
		backup_id = context['backup_id']
		self.task_manager.add_task(ShowBackupTask(source, backup_id))

	def cmd_rename(self, source: CommandSource, context: CommandContext):
		backup_id = context['backup_id']
		comment = context['comment']
		self.task_manager.add_task(RenameBackupTask(source, backup_id, comment))

	def cmd_delete(self, source: CommandSource, context: CommandContext):
		backup_id = context['backup_id']
		self.task_manager.add_task(DeleteBackupTask(source, backup_id))

	def cmd_delete_range(self, source: CommandSource, context: CommandContext):
		id_range: IdRangeNode.Range = context['backup_id_range']
		self.task_manager.add_task(DeleteBackupRangeTask(source, id_range.start, id_range.end))

	def cmd_export(self, source: CommandSource, context: CommandContext):
		backup_id = context['backup_id']
		export_format = context.get('export_format', ExportBackupFormat.tar)
		self.task_manager.add_task(ExportBackupTask(source, backup_id, export_format))

	def cmd_crontab_show(self, source: CommandSource, context: CommandContext):
		job_id = context.get('job_id', CrontabJobId.schedule_backup)
		self.task_manager.add_task(ShowCrontabJobTask(source, self.crontab_manager, job_id))

	def cmd_crontab_pause(self, source: CommandSource, context: CommandContext):
		job_id = context.get('job_id', CrontabJobId.schedule_backup)
		self.task_manager.add_task(OperateCrontabJobTask(source, self.crontab_manager, job_id, OperateCrontabJobTask.Operation.pause))

	def cmd_crontab_resume(self, source: CommandSource, context: CommandContext):
		job_id = context.get('job_id', CrontabJobId.schedule_backup)
		self.task_manager.add_task(OperateCrontabJobTask(source, self.crontab_manager, job_id, OperateCrontabJobTask.Operation.resume))

	def cmd_prune(self, source: CommandSource, context: CommandContext):
		self.task_manager.add_task(PruneAllBackupTask(source))

	def cmd_confirm(self, source: CommandSource, context: CommandContext):
		if not self.task_manager.do_confirm():
			reply_message(source, tr('command.confirm.noop'))

	def cmd_abort(self, source: CommandSource, context: CommandContext):
		if not self.task_manager.do_abort():
			reply_message(source, tr('command.abort.noop'))

	def suggest_backup_id(self) -> List[str]:
		return []  # TODO

	def register_commands(self):
		permissions = self.config.command.permission

		def get_permission_checker(literal: str) -> Callable[[CommandSource], bool]:
			return functools.partial(CommandSource.has_permission, level=permissions.get(literal))

		def get_permission_denied_text():
			return tr('error.permission_denied').set_color(RColor.red)

		builder = SimpleCommandBuilder()

		# simple commands

		builder.command('help', self.cmd_help)
		builder.command('help <what>', self.cmd_help)

		builder.command('make', self.cmd_make)
		builder.command('make <comment>', self.cmd_make)
		builder.command('back', self.cmd_back)
		builder.command('back --confirm', functools.partial(self.cmd_back, skip_confirm=True))
		builder.command('back <backup_id>', self.cmd_back)
		builder.command('back <backup_id> --confirm', functools.partial(self.cmd_back, skip_confirm=True))
		builder.command('show <backup_id>', self.cmd_show)
		builder.command('rename <backup_id> <comment>', self.cmd_rename)
		builder.command('delete <backup_id>', self.cmd_delete)
		builder.command('delete_range <backup_id_range>', self.cmd_delete_range)
		builder.command('export <backup_id>', self.cmd_export)
		builder.command('export <backup_id> <export_format>', self.cmd_export)
		builder.command('prune', self.cmd_prune)

		builder.command('crontab <job_id>', self.cmd_crontab_show)
		builder.command('crontab <job_id> pause', self.cmd_crontab_pause)
		builder.command('crontab <job_id> resume', self.cmd_crontab_resume)

		builder.command('confirm', self.cmd_confirm)
		builder.command('abort', self.cmd_abort)

		builder.arg('backup_id', Integer).suggests(self.suggest_backup_id)
		builder.arg('backup_id_range', IdRangeNode)
		builder.arg('comment', GreedyText)
		builder.arg('export_format', lambda n: Enumeration(n, ExportBackupFormat))
		builder.arg('job_id', lambda n: Enumeration(n, CrontabJobId))
		builder.arg('page', lambda n: Integer(n).at_min(1))
		builder.arg('per_page', lambda n: Integer(n).at_min(1))
		builder.arg('what', Text).suggests(lambda: self.COMMANDS_WITH_DETAILED_HELP)

		for name, level in permissions.items():
			builder.literal(name).requires(get_permission_checker(name), get_permission_denied_text)

		root = (
			Literal(self.config.command.prefix).
			requires(lambda: not self.plugin_disabled, lambda: tr('error.disabled').set_color(RColor.red)).
			runs(self.cmd_welcome)
		)
		builder.add_children_for(root)

		# complex commands

		def make_list_cmd() -> Literal:
			node = Literal('list')
			node.requires(get_permission_checker('list'), get_permission_denied_text)
			node.runs(self.cmd_list)
			node.then(Integer('page').at_min(1).redirects(node))
			node.then(Literal('--per-page').then(Integer('per_page').in_range(1, 20).redirects(node)))
			node.then(Literal('--author').then(QuotableText('author').redirects(node)))
			node.then(Literal('--start').then(DateNode('start_date').redirects(node)))
			node.then(Literal('--end').then(DateNode('end_date').redirects(node)))
			node.then(CountingLiteral('--all', 'all').redirects(node))
			node.then(CountingLiteral('--size', 'size').redirects(node))
			return node

		root.then(make_list_cmd())

		# register

		self.server.register_command(root)
