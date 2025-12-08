import enum
import functools
from concurrent import futures
from pathlib import Path
from typing import List, Callable, Optional, Type, Union, Any, Literal as TypingLiteral

from mcdreforged.api.all import PluginServerInterface, CommandSource, CommandContext, RText, RColor, AbstractNode, \
	Literal, ArgumentNode, Text, QuotableText, GreedyText, Integer, Enumeration, CountingLiteral, Boolean, Float, \
	SimpleCommandBuilder

from prime_backup.compressors import CompressMethod
from prime_backup.config.config import Config
from prime_backup.mcdr.command.nodes import DateNode, IdRangeNode, HexStringNode, JsonObjectNode, BackupIdNode, MultiBackupIdNode
from prime_backup.mcdr.command.value_suggestor import ValueSuggesters
from prime_backup.mcdr.crontab_job import CrontabJobEvent, CrontabJobId
from prime_backup.mcdr.crontab_manager import CrontabManager
from prime_backup.mcdr.task.backup.create_backup_task import CreateBackupTask
from prime_backup.mcdr.task.backup.delete_backup_task import DeleteBackupTask, DeleteBackupRangeTask
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
from prime_backup.mcdr.task.backup.transform_backup_id_task import TransformBackupIdTask
from prime_backup.mcdr.task.crontab.list_crontab_task import ListCrontabJobTask
from prime_backup.mcdr.task.crontab.operate_crontab_task import OperateCrontabJobTask
from prime_backup.mcdr.task.crontab.show_crontab_task import ShowCrontabJobTask
from prime_backup.mcdr.task.db.delete_backup_file_task import DeleteBackupFileTask
from prime_backup.mcdr.task.db.inspect_object_tasks import InspectBackupTask, InspectBackupFileTask, InspectBlobTask, InspectFilesetTask, InspectFilesetFileTask
from prime_backup.mcdr.task.db.migrate_compress_method_task import MigrateCompressMethodTask
from prime_backup.mcdr.task.db.migrate_hash_method_task import MigrateHashMethodTask
from prime_backup.mcdr.task.db.prune_database_task import PruneDatabaseTask
from prime_backup.mcdr.task.db.show_db_overview_task import ShowDbOverviewTask
from prime_backup.mcdr.task.db.vacuum_sqlite_task import VacuumSqliteTask
from prime_backup.mcdr.task.db.validate_db_task import ValidateDbTask, ValidatePart
from prime_backup.mcdr.task.general.show_help_task import ShowHelpTask
from prime_backup.mcdr.task.general.show_welcome_task import ShowWelcomeTask
from prime_backup.mcdr.task_manager import TaskManager
from prime_backup.types.backup_filter import BackupFilter, BackupSortOrder
from prime_backup.types.backup_tags import BackupTagName
from prime_backup.types.hash_method import HashMethod
from prime_backup.types.operator import Operator
from prime_backup.types.standalone_backup_format import StandaloneBackupFormat
from prime_backup.utils import misc_utils
from prime_backup.utils.mcdr_utils import tr, reply_message, mkcmd


class CommandManagerState(enum.Enum):
	INITIAL = enum.auto()
	HOOKED = enum.auto()
	READY = enum.auto()
	DISABLED = enum.auto()


class CommandManager:
	def __init__(self, server: PluginServerInterface, task_manager: TaskManager, crontab_manager: CrontabManager):
		self.server = server
		self.task_manager = task_manager
		self.crontab_manager = crontab_manager
		self.value_suggesters = ValueSuggesters(task_manager)
		self.config = Config.get()
		self.__state = CommandManagerState.INITIAL
		self.__root_node = Literal(self.config.command.prefix)

	def close_the_door(self):
		self.__state = CommandManagerState.DISABLED

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
		def backup_id_consumer(backup_id: int):
			self.task_manager.add_task(InspectBackupTask(source, backup_id))
		self.transform_backup_id(source, context['backup_id'], backup_id_consumer)

	def cmd_db_inspect_backup_file(self, source: CommandSource, context: CommandContext):
		def backup_id_consumer(backup_id: int):
			file_path = context['backup_file_path']
			self.task_manager.add_task(InspectBackupFileTask(source, backup_id, file_path))
		self.transform_backup_id(source, context['backup_id'], backup_id_consumer)

	def cmd_db_inspect_fileset_file(self, source: CommandSource, context: CommandContext):
		fileset_id = context['fileset_id']
		file_path = context['fileset_file_path']
		self.task_manager.add_task(InspectFilesetFileTask(source, fileset_id, file_path))

	def cmd_db_inspect_fileset(self, source: CommandSource, context: CommandContext):
		fileset_id = context['fileset_id']
		self.task_manager.add_task(InspectFilesetTask(source, fileset_id))

	def cmd_db_inspect_blob(self, source: CommandSource, context: CommandContext):
		blob_hash = context['blob_hash']
		self.task_manager.add_task(InspectBlobTask(source, blob_hash))

	def cmd_db_delete_file(self, source: CommandSource, context: CommandContext):
		def backup_id_consumer(backup_id: int):
			file_path = context['backup_file_path']
			needs_confirm = context.get('confirm', 0) == 0
			recursive = context.get('recursive', 0) > 0
			self.task_manager.add_task(DeleteBackupFileTask(source, backup_id, file_path, needs_confirm=needs_confirm, recursive=recursive))
		self.transform_backup_id(source, context['backup_id'], backup_id_consumer)

	def cmd_db_validate(self, source: CommandSource, _: CommandContext, parts: ValidatePart):
		self.task_manager.add_task(ValidateDbTask(source, parts))

	def cmd_db_vacuum(self, source: CommandSource, _: CommandContext):
		self.task_manager.add_task(VacuumSqliteTask(source))

	def cmd_db_migrate_compress_method(self, source: CommandSource, context: CommandContext):
		new_compress_method = context['compress_method']
		self.task_manager.add_task(MigrateCompressMethodTask(source, new_compress_method))

	def cmd_db_migrate_hash_method(self, source: CommandSource, context: CommandContext):
		new_hash_method = context['hash_method']
		self.task_manager.add_task(MigrateHashMethodTask(source, new_hash_method))

	def cmd_db_prune(self, source: CommandSource, _: CommandContext):
		self.task_manager.add_task(PruneDatabaseTask(source))

	def cmd_make(self, source: CommandSource, context: CommandContext):
		def callback(backup_id: Optional[int], err: Optional[Exception]):
			if err is None and backup_id is not None:
				self.crontab_manager.send_event(CrontabJobEvent.manual_backup_created)

		comment = context.get('comment', '')
		self.task_manager.add_task(CreateBackupTask(source, comment), callback)

	def cmd_back(self, source: CommandSource, context: CommandContext):
		def backup_id_consumer(backup_id: Optional[int]):
			needs_confirm = context.get('confirm', 0) == 0
			fail_soft = context.get('fail_soft', 0) > 0
			verify_blob = context.get('no_verify', 0) == 0
			self.task_manager.add_task(RestoreBackupTask(source, backup_id, needs_confirm=needs_confirm, fail_soft=fail_soft, verify_blob=verify_blob))
		self.transform_backup_id_opt(source, context.get('backup_id'), backup_id_consumer)

	def cmd_list(self, source: CommandSource, context: CommandContext):
		page = context.get('page', 1)
		per_page = context.get('per_page', 10)

		backup_filter = BackupFilter()
		if (sort_order := context.get('sort_order')) is not None:
			backup_filter.sort_order = sort_order
		if (start_date := context.get('start_date')) is not None:
			backup_filter.timestamp_us_start = int(start_date)
		if (end_date := context.get('end_date')) is not None:
			backup_filter.timestamp_us_end = int(end_date)
		if context.get('me', 0) > 0:
			backup_filter.creator = Operator.of(source)
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
		def backup_id_consumer(backup_id: int):
			self.task_manager.add_task(ShowBackupTask(source, backup_id))
		self.transform_backup_id(source, context['backup_id'], backup_id_consumer)

	def cmd_rename(self, source: CommandSource, context: CommandContext):
		def backup_id_consumer(backup_id: int):
			comment = context['comment']
			self.task_manager.add_task(RenameBackupTask(source, backup_id, comment))
		self.transform_backup_id(source, context['backup_id'], backup_id_consumer)

	def cmd_delete(self, source: CommandSource, context: CommandContext):
		def backup_ids_consumer(backup_ids: List[int]):
			needs_confirm = context.get('confirm', 0) == 0
			self.task_manager.add_task(DeleteBackupTask(source, backup_ids, needs_confirm))

		if 'backup_id' not in context:
			reply_message(source, tr('error.missing_backup_id').set_color(RColor.red))
			return
		self.transform_backup_ids(source, context['backup_id'], backup_ids_consumer)

	def cmd_delete_range(self, source: CommandSource, context: CommandContext):
		id_range: IdRangeNode.Range = context['backup_id_range']
		self.task_manager.add_task(DeleteBackupRangeTask(source, id_range.start, id_range.end))

	def cmd_export(self, source: CommandSource, context: CommandContext):
		def backup_id_consumer(backup_id: int):
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
		self.transform_backup_id(source, context['backup_id'], backup_id_consumer)

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
		def backup_id_consumer(backup_ids: List[int]):
			if len(backup_ids) != 2:
				raise AssertionError(repr(backup_ids))
			backup_id_old, backup_id_new = backup_ids
			self.task_manager.add_task(DiffBackupTask(source, backup_id_old, backup_id_new))
		self.transform_backup_ids(source, [context['backup_id_old'], context['backup_id_new']], backup_id_consumer)

	def cmd_confirm(self, source: CommandSource, _: CommandContext):
		self.task_manager.do_confirm(source)

	def cmd_abort(self, source: CommandSource, _: CommandContext):
		self.task_manager.do_abort(source)

	def cmd_show_backup_tag(self, source: CommandSource, context: CommandContext, tag_name: Optional[BackupTagName] = None):
		def backup_id_consumer(backup_id: int):
			if tag_name is not None:
				self.task_manager.add_task(ShowBackupSingleTagTask(source, backup_id, tag_name))
			else:
				self.task_manager.add_task(ShowBackupTagTask(source, backup_id))
		self.transform_backup_id(source, context['backup_id'], backup_id_consumer)

	def cmd_operate_backup_tag(self, source: CommandSource, context: CommandContext, tag_name: BackupTagName, mode: TypingLiteral['set', 'clear']):
		def backup_id_consumer(backup_id: int):
			if mode == 'set':
				value = context['value']
				self.task_manager.add_task(SetBackupTagTask(source, backup_id, tag_name, value))
			elif mode == 'clear':
				self.task_manager.add_task(ClearBackupTagTask(source, backup_id, tag_name))
			else:
				raise ValueError(mode)
		self.transform_backup_id(source, context['backup_id'], backup_id_consumer)

	# ============================ Command Callback ends ============================

	def suggest_backup_id(self, source: CommandSource) -> List[str]:
		return [
			*BackupIdNode.get_command_suggestions(),
			*self.value_suggesters.suggest_backup_id(source),
		]

	def suggest_fileset_id(self, source: CommandSource) -> List[str]:
		return self.value_suggesters.suggest_fileset_id(source)

	def suggest_backup_file_path(self, source: CommandSource, ctx: CommandContext) -> List[str]:
		return self.value_suggesters.suggest_backup_file_path(source, ctx['backup_id'])

	def suggest_fileset_file_path(self, source: CommandSource, ctx: CommandContext) -> List[str]:
		return self.value_suggesters.suggest_fileset_file_path(source, ctx['fileset_id'])

	def __transform_backup_id_impl(self, source: CommandSource, backup_id_raw: Union[str, List[str]], csm: Union[Callable[[int], Any], Callable[[List[int]], Any]]):
		if isinstance(backup_id_raw, str):
			backup_id_strings = [backup_id_raw]
		elif isinstance(backup_id_raw, list):
			backup_id_strings = [misc_utils.ensure_type(s, str) for s in backup_id_raw]
		else:
			raise TypeError(type(backup_id_raw))

		def process_task_result(parsed_backup_ids: List[int]):
			if isinstance(backup_id_raw, str):
				csm(parsed_backup_ids[0])
			else:
				csm(parsed_backup_ids)

		bid_task = TransformBackupIdTask(source, backup_id_strings)
		if bid_task.needs_db_access():
			def done_callback(future: 'futures.Future[List[int]]'):
				if not future.exception():
					process_task_result(future.result())
			self.task_manager.add_task(bid_task).add_done_callback(done_callback)
		else:
			process_task_result(bid_task.run(allow_db_access=False))

	def transform_backup_id_opt(self, source: CommandSource, backup_id_raw: Optional[str], csm: Callable[[Optional[int]], Any]):
		if backup_id_raw is None:
			csm(None)
		else:
			self.transform_backup_id(source, backup_id_raw, csm)

	def transform_backup_id(self, source: CommandSource, backup_id_raw: str, csm: Callable[[int], Any]):
		self.__transform_backup_id_impl(source, backup_id_raw, csm)

	def transform_backup_ids(self, source: CommandSource, backup_id_raw: List[str], csm: Callable[[List[int]], Any]):
		self.__transform_backup_id_impl(source, backup_id_raw, csm)

	def register_command_node(self):
		if self.__state != CommandManagerState.INITIAL:
			raise AssertionError(self.__state)

		self.__root_node.requires(
			lambda: self.__state == CommandManagerState.READY,
			lambda: tr('error.disabled' if self.__state == CommandManagerState.DISABLED else 'error.initializing').set_color(RColor.red),
		)
		self.server.register_command(self.__root_node)
		self.__state = CommandManagerState.HOOKED

	def construct_command_tree(self):
		if self.__state != CommandManagerState.HOOKED:
			raise AssertionError(self.__state)

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

		def create_backup_id(arg_name: str = 'backup_id', clazz: Type[ArgumentNode] = BackupIdNode) -> ArgumentNode:
			return clazz(arg_name).suggests(self.suggest_backup_id)

		def create_fileset_id(arg_name: str) -> ArgumentNode:
			return Integer(arg_name).suggests(self.suggest_fileset_id)

		def create_backup_file_path(arg_name: str) -> ArgumentNode:
			return QuotableText(arg_name).suggests(self.suggest_backup_file_path)

		def create_fileset_file_path(arg_name: str) -> ArgumentNode:
			return QuotableText(arg_name).suggests(self.suggest_fileset_file_path)

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
		builder.command('database inspect file <backup_id> <backup_file_path>', self.cmd_db_inspect_backup_file)
		builder.command('database inspect file2 <fileset_id> <fileset_file_path>', self.cmd_db_inspect_fileset_file)
		builder.command('database inspect fileset <fileset_id>', self.cmd_db_inspect_fileset)
		builder.command('database inspect blob <blob_hash>', self.cmd_db_inspect_blob)
		builder.command('database validate all', functools.partial(self.cmd_db_validate, parts=ValidatePart.all()))
		builder.command('database validate blobs', functools.partial(self.cmd_db_validate, parts=ValidatePart.blobs))
		builder.command('database validate files', functools.partial(self.cmd_db_validate, parts=ValidatePart.files))
		builder.command('database validate filesets', functools.partial(self.cmd_db_validate, parts=ValidatePart.filesets))
		builder.command('database validate backups', functools.partial(self.cmd_db_validate, parts=ValidatePart.backups))
		builder.command('database vacuum', self.cmd_db_vacuum)
		builder.command('database prune', self.cmd_db_prune)
		builder.command('database migrate_compress_method <compress_method>', self.cmd_db_migrate_compress_method)
		builder.command('database migrate_hash_method <hash_method>', self.cmd_db_migrate_hash_method)
		# `database delete file <backup_id> <backup_file_path>` is handled by `make_db_delete_file_cmd()` below

		builder.arg('fileset_id', create_fileset_id)  # not that necessary to provide suggestion here
		builder.arg('backup_file_path', create_backup_file_path)  # not that necessary to provide suggestion here
		builder.arg('fileset_file_path', create_fileset_file_path)  # not that necessary to provide suggestion here
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
			self.__root_node.
			requires(get_permission_checker('root'), get_permission_denied_text).
			runs(self.cmd_welcome)
		)
		builder.add_children_for(root)

		# --------------- complex commands ---------------

		def __locate_node(literal_path: List[str]) -> Literal:
			current_node = root
			for ltr in literal_path:
				for child in current_node.get_children():
					if isinstance(child, Literal) and ltr in child.literals:
						current_node = child
						break
				else:
					raise KeyError('Node {} does not have literal {!r}'.format(current_node, ltr))
			return current_node

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
			node_bid = create_backup_id(clazz=MultiBackupIdNode).redirects(node_sc)
			node_sc.then(node_bid)
			set_confirm_able(node_sc)
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
			node.then(Literal('--sort').then(Enumeration('sort_order', BackupSortOrder).redirects(node)))
			node.then(Literal('--creator').then(QuotableText('creator').redirects(node)))
			node.then(CountingLiteral('--me', 'me').redirects(node))
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

		def make_db_delete_file_cmd():
			__locate_node(['database']).then(Literal('delete').then(node_subcommand := Literal('file')))
			node_bid = create_backup_id()
			node_file_path = create_backup_file_path('backup_file_path').runs(self.cmd_db_delete_file)
			node_subcommand.then(node_bid)
			node_bid.then(node_file_path)
			for node in [node_bid, node_file_path]:
				set_confirm_able(node)
				node.then(CountingLiteral('--recursive', 'recursive').redirects(node))

		# backup
		root.then(make_back_cmd())
		root.then(make_delete_cmd())
		root.then(make_export_cmd())
		root.then(make_import_cmd())
		root.then(make_list_cmd())
		root.then(make_tag_cmd())
		make_db_delete_file_cmd()

		# --------------- done ---------------

		self.__state = CommandManagerState.READY
