import functools
import typing
from pathlib import Path
from typing import List, Callable, Optional, Type

from mcdreforged.api.all import *

from prime_backup.compressors import CompressMethod
from prime_backup.config.config import Config
from prime_backup.mcdr.command.backup_id_suggestor import BackupIdSuggestor
from prime_backup.mcdr.command.nodes import DateNode, IdRangeNode, MultiIntegerNode, HexStringNode, JsonObjectNode
from prime_backup.mcdr.crontab_job import CrontabJobEvent, CrontabJobId
from prime_backup.mcdr.crontab_manager import CrontabManager
from prime_backup.mcdr.task.backup.create_backup_task import CreateBackupTask
from prime_backup.mcdr.task.backup.delete_backup_range_task import DeleteBackupRangeTask
from prime_backup.mcdr.task.backup.delete_backup_task import DeleteBackupTask
from prime_backup.mcdr.task.backup.diff_backup_task import DiffBackupTask
from prime_backup.mcdr.task.backup.export_backup_task import ExportBackupTask
from prime_backup.mcdr.task.backup.import_backup_task import ImportBackupTask
from prime_backup.mcdr.task.backup.list_backup_task import ListBackupTask
from prime_backup.mcdr.task.backup.operate_backup_tag_task import SetBackupTagTask, ClearBackupTagTask
from prime_backup.mcdr.task.backup.prune_backup_task import PruneAllBackupTask
from prime_backup.mcdr.task.backup.rename_backup_task import RenameBackupTask
from prime_backup.mcdr.task.backup.restore_backup_task import RestoreBackupTask
from prime_backup.mcdr.task.backup.show_backup_tag_task import ShowBackupTagTask, ShowBackupSingleTagTask
from prime_backup.mcdr.task.backup.show_backup_task import ShowBackupTask
from prime_backup.mcdr.task.crontab.list_crontab_task import ListCrontabJobTask
from prime_backup.mcdr.task.crontab.operate_crontab_task import OperateCrontabJobTask
from prime_backup.mcdr.task.crontab.show_crontab_task import ShowCrontabJobTask
from prime_backup.mcdr.task.db.inspect_object_tasks import InspectBackupTask, InspectFileTask, InspectBlobTask
from prime_backup.mcdr.task.db.migrate_compress_method_task import MigrateCompressMethodTask
from prime_backup.mcdr.task.db.migrate_hash_method_task import MigrateHashMethodTask
from prime_backup.mcdr.task.db.show_db_overview_task import ShowDbOverviewTask
from prime_backup.mcdr.task.db.vacuum_sqlite_task import VacuumSqliteTask
from prime_backup.mcdr.task.db.validate_db_task import ValidateDbTask, ValidateParts
from prime_backup.mcdr.task.general.show_help_task import ShowHelpTask
from prime_backup.mcdr.task.general.show_welcome_task import ShowWelcomeTask
from prime_backup.mcdr.task_manager import TaskManager
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.types.backup_tags import BackupTagName
from prime_backup.types.hash_method import HashMethod
from prime_backup.types.operator import Operator
from prime_backup.types.standalone_backup_format import StandaloneBackupFormat
from prime_backup.utils import misc_utils
from prime_backup.utils.mcdr_utils import tr, reply_message, mkcmd
from prime_backup.utils.waitable_value import WaitableValue


class CommandManager:
	def __init__(self, server: PluginServerInterface, task_manager: TaskManager, crontab_manager: CrontabManager):
		self.server = server
		self.task_manager = task_manager
		self.crontab_manager = crontab_manager
		self.backup_id_suggestor = BackupIdSuggestor(task_manager)
		self.config = Config.get()
		self.plugin_disabled = False

	def close_the_door(self):
		self.plugin_disabled = True

	# =============================== Command Callback ===============================

	def cmd_welcome(self, source: CommandSource, _: CommandContext):
		self.task_manager.add_task(ShowWelcomeTask(source))

	def cmd_help(self, source: CommandSource, context: dict):
		what = context.get('what')
		if what is not None and what not in ShowHelpTask.COMMANDS_WITH_DETAILED_HELP:
			reply_message(source, tr('command.help.no_help', RText(mkcmd(what), RColor.gray)))
			return

		self.task_manager.add_task(ShowHelpTask(source, what))

	def cmd_db_overview(self, source: CommandSource, _: CommandContext):
		self.task_manager.add_task(ShowDbOverviewTask(source))

	def cmd_db_inspect_backup(self, source: CommandSource, context: CommandContext):
		backup_id = context['backup_id']
		self.task_manager.add_task(InspectBackupTask(source, backup_id))

	def cmd_db_inspect_file(self, source: CommandSource, context: CommandContext):
		backup_id = context['backup_id']
		file_path = context['file_path']
		self.task_manager.add_task(InspectFileTask(source, backup_id, file_path))

	def cmd_db_inspect_blob(self, source: CommandSource, context: CommandContext):
		blob_hash = context['blob_hash']
		self.task_manager.add_task(InspectBlobTask(source, blob_hash))

	def cmd_db_validate(self, source: CommandSource, _: CommandContext, parts: ValidateParts):
		self.task_manager.add_task(ValidateDbTask(source, parts))

	def cmd_db_vacuum(self, source: CommandSource, _: CommandContext):
		self.task_manager.add_task(VacuumSqliteTask(source))

	def cmd_db_migrate_compress_method(self, source: CommandSource, context: CommandContext):
		new_compress_method = context['compress_method']
		self.task_manager.add_task(MigrateCompressMethodTask(source, new_compress_method))

	def cmd_db_migrate_hash_method(self, source: CommandSource, context: CommandContext):
		new_hash_method = context['hash_method']
		self.task_manager.add_task(MigrateHashMethodTask(source, new_hash_method))

	def cmd_make(self, source: CommandSource, context: CommandContext):
		def callback(_, err):
			if err is None:
				self.crontab_manager.send_event(CrontabJobEvent.manual_backup_created)

		comment = context.get('comment', '')
		self.task_manager.add_task(CreateBackupTask(source, comment), callback)

	def cmd_back(self, source: CommandSource, context: CommandContext):
		needs_confirm = context.get('confirm', 0) == 0
		fail_soft = context.get('fail_soft', 0) > 0
		verify_blob = context.get('no_verify', 0) == 0
		backup_id = context.get('backup_id')
		self.task_manager.add_task(RestoreBackupTask(source, backup_id, needs_confirm=needs_confirm, fail_soft=fail_soft, verify_blob=verify_blob))

	def cmd_list(self, source: CommandSource, context: CommandContext):
		page = context.get('page', 1)
		per_page = context.get('per_page', 10)

		backup_filter = BackupFilter()
		if (start_date := context.get('start_date')) is not None:
			backup_filter.timestamp_start = int(start_date)
		if (end_date := context.get('end_date')) is not None:
			backup_filter.timestamp_end = int(end_date)
		if (creator_str := context.get('creator')) is not None:
			if ':' in creator_str:
				creator = Operator.of(creator_str)
			else:
				creator = Operator.player(creator_str)
			backup_filter.creator = creator
		show_all = context.get('all', 0) > 0
		show_flags = context.get('flags', 0) > 0

		self.task_manager.add_task(ListBackupTask(source, per_page, page, backup_filter, show_all, show_flags))

	def cmd_show(self, source: CommandSource, context: CommandContext):
		backup_id = context['backup_id']
		self.task_manager.add_task(ShowBackupTask(source, backup_id))

	def cmd_rename(self, source: CommandSource, context: CommandContext):
		backup_id = context['backup_id']
		comment = context['comment']
		self.task_manager.add_task(RenameBackupTask(source, backup_id, comment))

	def cmd_delete(self, source: CommandSource, context: CommandContext):
		if 'backup_id' not in context:
			reply_message(source, tr('error.missing_backup_id').set_color(RColor.red))
			return

		backup_ids = misc_utils.ensure_type(context['backup_id'], list)
		self.task_manager.add_task(DeleteBackupTask(source, backup_ids))

	def cmd_delete_range(self, source: CommandSource, context: CommandContext):
		id_range: IdRangeNode.Range = context['backup_id_range']
		self.task_manager.add_task(DeleteBackupRangeTask(source, id_range.start, id_range.end))

	def cmd_export(self, source: CommandSource, context: CommandContext):
		backup_id = context['backup_id']
		export_format = context.get('export_format', StandaloneBackupFormat.tar)
		fail_soft = context.get('fail_soft', 0) > 0
		verify_blob = context.get('no_verify', 0) == 0
		overwrite_existing = context.get('overwrite', 0) > 0
		create_meta = context.get('no_meta', 0) == 0
		self.task_manager.add_task(ExportBackupTask(
			source, backup_id, export_format,
			fail_soft=fail_soft,
			verify_blob=verify_blob,
			overwrite_existing=overwrite_existing,
			create_meta=create_meta,
		))

	def cmd_import(self, source: CommandSource, context: CommandContext):
		file_path = Path(context['file_path'])
		backup_format = context.get('backup_format')
		ensure_meta = context.get('auto_meta', 0) == 0
		meta_override = context.get('meta_override')
		self.task_manager.add_task(ImportBackupTask(source, file_path, backup_format, ensure_meta=ensure_meta, meta_override=meta_override))

	def cmd_crontab_show(self, source: CommandSource, context: CommandContext):
		job_id = context.get('job_id')
		if job_id is None:
			self.task_manager.add_task(ListCrontabJobTask(source, self.crontab_manager))
		else:
			self.task_manager.add_task(ShowCrontabJobTask(source, self.crontab_manager, job_id))

	def cmd_crontab_pause(self, source: CommandSource, context: CommandContext):
		job_id = context.get('job_id', CrontabJobId.schedule_backup)
		self.task_manager.add_task(OperateCrontabJobTask(source, self.crontab_manager, job_id, OperateCrontabJobTask.Operation.pause))

	def cmd_crontab_resume(self, source: CommandSource, context: CommandContext):
		job_id = context.get('job_id', CrontabJobId.schedule_backup)
		self.task_manager.add_task(OperateCrontabJobTask(source, self.crontab_manager, job_id, OperateCrontabJobTask.Operation.resume))

	def cmd_prune(self, source: CommandSource, _: CommandContext):
		self.task_manager.add_task(PruneAllBackupTask(source))

	def cmd_diff(self, source: CommandSource, context: CommandContext):
		backup_id_old = context['backup_id_old']
		backup_id_new = context['backup_id_new']
		self.task_manager.add_task(DiffBackupTask(source, backup_id_old, backup_id_new))

	def cmd_confirm(self, source: CommandSource, _: CommandContext):
		self.task_manager.do_confirm(source)

	def cmd_abort(self, source: CommandSource, _: CommandContext):
		self.task_manager.do_abort(source)

	def cmd_show_backup_tag(self, source: CommandSource, context: CommandContext, tag_name: Optional[BackupTagName] = None):
		backup_id = context['backup_id']
		if tag_name is not None:
			self.task_manager.add_task(ShowBackupSingleTagTask(source, backup_id, tag_name))
		else:
			self.task_manager.add_task(ShowBackupTagTask(source, backup_id))

	def cmd_operate_backup_tag(self, source: CommandSource, context: CommandContext, tag_name: BackupTagName, mode: typing.Literal['set', 'clear']):
		backup_id = context['backup_id']
		if mode == 'set':
			value = context['value']
			self.task_manager.add_task(SetBackupTagTask(source, backup_id, tag_name, value))
		elif mode == 'clear':
			self.task_manager.add_task(ClearBackupTagTask(source, backup_id, tag_name))
		else:
			raise ValueError(mode)

	# ============================ Command Callback ends ============================

	def suggest_backup_id(self, source: CommandSource) -> List[str]:
		wv = self.backup_id_suggestor.request(source)
		if wv.wait(0.2) == WaitableValue.EMPTY:
			return []
		return list(map(str, wv.get()))

	def register_commands(self):
		# --------------- common utils ---------------

		permissions = self.config.command.permission

		def get_permission_checker(literal: str) -> Callable[[CommandSource], bool]:
			return functools.partial(CommandSource.has_permission, level=permissions.get(literal))

		def get_permission_denied_text():
			return tr('error.permission_denied').set_color(RColor.red)

		def create_subcommand(literal: str) -> Literal:
			node = Literal(literal)
			node.requires(get_permission_checker(literal), get_permission_denied_text)
			return node

		def create_backup_id(arg_name: str = 'backup_id', clazz: Type[Integer] = Integer) -> Integer:
			return clazz(arg_name).at_min(1).suggests(self.suggest_backup_id)

		# --------------- simple commands ---------------

		builder = SimpleCommandBuilder()

		# help

		builder.command('help', self.cmd_help)
		builder.command('help <what>', self.cmd_help)

		builder.arg('what', Text).suggests(lambda: ShowHelpTask.COMMANDS_WITH_DETAILED_HELP)

		# backup
		builder.command('make', self.cmd_make)
		builder.command('make <comment>', self.cmd_make)
		builder.command('show <backup_id>', self.cmd_show)
		builder.command('rename <backup_id> <comment>', self.cmd_rename)
		builder.command('delete_range <backup_id_range>', self.cmd_delete_range)
		builder.command('prune', self.cmd_prune)
		builder.command('diff <backup_id_old> <backup_id_new>', self.cmd_diff)

		for arg in ['backup_id', 'backup_id_old', 'backup_id_new']:
			builder.arg(arg, create_backup_id)
		builder.arg('backup_id_range', IdRangeNode)
		builder.arg('comment', GreedyText)
		builder.arg('file_path', QuotableText)

		# crontab
		builder.command('crontab', self.cmd_crontab_show)
		builder.command('crontab <job_id>', self.cmd_crontab_show)
		builder.command('crontab <job_id> pause', self.cmd_crontab_pause)
		builder.command('crontab <job_id> resume', self.cmd_crontab_resume)

		builder.arg('job_id', lambda n: Enumeration(n, CrontabJobId))

		# db
		builder.command('database', lambda src: self.cmd_help(src, {'what': 'database'}))
		builder.command('database overview', self.cmd_db_overview)
		builder.command('database inspect backup <backup_id>', self.cmd_db_inspect_backup)
		builder.command('database inspect file <backup_id> <file_path>', self.cmd_db_inspect_file)
		builder.command('database inspect blob <blob_hash>', self.cmd_db_inspect_blob)
		builder.command('database validate all', functools.partial(self.cmd_db_validate, parts=ValidateParts.all()))
		builder.command('database validate blobs', functools.partial(self.cmd_db_validate, parts=ValidateParts.blobs))
		builder.command('database validate files', functools.partial(self.cmd_db_validate, parts=ValidateParts.files))
		builder.command('database vacuum', self.cmd_db_vacuum)
		builder.command('database migrate_compress_method <compress_method>', self.cmd_db_migrate_compress_method)
		builder.command('database migrate_hash_method <hash_method>', self.cmd_db_migrate_hash_method)

		builder.arg('file_path', QuotableText)  # Notes: it's actually a redefine
		builder.arg('blob_hash', HexStringNode)
		builder.arg('compress_method', lambda n: Enumeration(n, CompressMethod))
		builder.arg('hash_method', lambda n: Enumeration(n, HashMethod))

		# operations
		builder.command('confirm', self.cmd_confirm)
		builder.command('abort', self.cmd_abort)

		# subcommand permissions
		for name, level in permissions.items():
			builder.literal(name).requires(get_permission_checker(name), get_permission_denied_text)

		root = (
			Literal(self.config.command.prefix).
			requires(lambda: not self.plugin_disabled, lambda: tr('error.disabled').set_color(RColor.red)).
			requires(get_permission_checker('root'), get_permission_denied_text).
			runs(self.cmd_welcome)
		)
		builder.add_children_for(root)

		# --------------- complex commands ---------------

		def set_confirm_able(node: AbstractNode):
			node.then(CountingLiteral('--confirm', 'confirm').redirects(node))

		def set_fail_soft_able(node: AbstractNode):
			node.then(CountingLiteral('--fail-soft', 'fail_soft').redirects(node))

		def set_no_verify_able(node: AbstractNode):
			node.then(CountingLiteral('--no-verify', 'no_verify').redirects(node))

		def make_back_cmd() -> Literal:
			node_sc = create_subcommand('back')
			node_bid = create_backup_id()
			node_sc.then(node_bid)
			for node in [node_sc, node_bid]:
				set_confirm_able(node)
				set_fail_soft_able(node)
				set_no_verify_able(node)
				node.runs(self.cmd_back)
			return node_sc

		def make_delete_cmd() -> Literal:
			node_sc = create_subcommand('delete').runs(self.cmd_delete)
			node_bid = create_backup_id(clazz=MultiIntegerNode).redirects(node_sc)
			node_sc.then(node_bid)
			return node_sc

		def make_export_cmd() -> Literal:
			node_sc = create_subcommand('export')
			node_bid = create_backup_id()
			node_ef = Enumeration('export_format', StandaloneBackupFormat)

			node_sc.then(node_bid)
			node_bid.then(node_ef)

			for node in [node_bid, node_ef]:
				set_fail_soft_able(node)
				set_no_verify_able(node)
				node.then(CountingLiteral('--overwrite', 'overwrite').redirects(node))
				node.then(CountingLiteral('--no-meta', 'no_meta').redirects(node))
				node.runs(self.cmd_export)

			return node_sc

		def make_import_cmd() -> Literal:
			node_sc = create_subcommand('import')
			node_fp = QuotableText('file_path')
			node_bf = Enumeration('backup_format', StandaloneBackupFormat)

			node_sc.then(node_fp)
			node_fp.then(node_bf)
			for node in [node_fp, node_bf]:
				node.then(CountingLiteral('--auto-meta', 'auto_meta').redirects(node))
				node.then(Literal('--meta-override').then(JsonObjectNode('meta_override').redirects(node)))
				node.runs(self.cmd_import)
			return node_sc

		def make_list_cmd() -> Literal:
			node = create_subcommand('list')
			node.runs(self.cmd_list)
			node.then(Integer('page').at_min(1).redirects(node))
			node.then(Literal('--per-page').then(Integer('per_page').in_range(1, 1000).redirects(node)))
			node.then(Literal('--creator').then(QuotableText('creator').redirects(node)))
			node.then(Literal('--from').then(DateNode('start_date').redirects(node)))
			node.then(Literal('--to').then(DateNode('end_date').redirects(node)))
			node.then(CountingLiteral('-a', 'all').redirects(node))
			node.then(CountingLiteral('--all', 'all').redirects(node))
			node.then(CountingLiteral('--flag', 'flags').redirects(node))
			node.then(CountingLiteral('--flags', 'flags').redirects(node))
			return node

		def make_tag_cmd() -> Literal:
			node = create_backup_id().runs(self.cmd_show_backup_tag)
			for tag_name in BackupTagName:
				arg_type = {
					bool: Boolean,
					int: Integer,
					float: Float,
					str: QuotableText,
				}[tag_name.value.type]

				bldr = SimpleCommandBuilder()
				bldr.command(f'{tag_name.name}', functools.partial(self.cmd_show_backup_tag, tag_name=tag_name))
				bldr.command(f'{tag_name.name} set <value>', functools.partial(self.cmd_operate_backup_tag, tag_name=tag_name, mode='set'))
				bldr.command(f'{tag_name.name} clear', functools.partial(self.cmd_operate_backup_tag, tag_name=tag_name, mode='clear'))
				bldr.arg('value', arg_type)
				children = bldr.build()
				misc_utils.assert_true(len(children) == 1, 'should build only 1 node')

				node.then(children[0])
			return create_subcommand('tag').then(node)

		# backup
		root.then(make_back_cmd())
		root.then(make_delete_cmd())
		root.then(make_export_cmd())
		root.then(make_import_cmd())
		root.then(make_list_cmd())
		root.then(make_tag_cmd())

		# --------------- register ---------------

		self.server.register_command(root)
