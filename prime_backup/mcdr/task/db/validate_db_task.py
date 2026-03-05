import enum
import logging
import time
from typing import List, Optional, TypeVar, Tuple, Callable, Dict, Union

from mcdreforged.api.all import CommandSource, RTextBase, RTextList, RColor, RAction
from typing_extensions import override, Protocol

from prime_backup.action import Action
from prime_backup.action.get_object_counts_action import GetObjectCountsAction, ObjectCounts
from prime_backup.action.validate_backups_action import ValidateBackupsAction, BadBackupItem
from prime_backup.action.validate_blob_chunk_group_bindings_action import BadBlobChunkGroupBindingItem, ValidateBlobChunkGroupBindingsResult
from prime_backup.action.validate_blobs_action import ValidateBlobsAction, BadBlobItem
from prime_backup.action.validate_chunk_group_chunk_bindings_action import BadChunkGroupChunkBindingItem, ValidateChunkGroupChunkBindingsResult
from prime_backup.action.validate_chunk_groups_action import BadChunkGroupItem, ValidateChunkGroupsResult
from prime_backup.action.validate_chunk_objects_action import ValidateChunkObjectsAction
from prime_backup.action.validate_chunks_action import BadChunkItem, ValidateChunksResult
from prime_backup.action.validate_files_action import ValidateFilesAction, BadFileItemType
from prime_backup.action.validate_filesets_action import ValidateFilesetsAction, BadFilesetItemType
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.file_info import FileInfo
from prime_backup.types.fileset_info import FilesetInfo
from prime_backup.utils import log_utils


class ValidatePart(enum.Flag):
	blobs = enum.auto()
	chunks = enum.auto()
	files = enum.auto()
	filesets = enum.auto()
	backups = enum.auto()

	@classmethod
	def all(cls) -> 'ValidatePart':
		flags = ValidatePart(0)
		for flag in ValidatePart:
			flags |= flag
		return flags


_T = TypeVar('_T')
_Action = TypeVar('_Action', bound=Action)


class _ResultWithFileAndBackupSamples(Protocol):
	affected_file_count: int
	affected_file_samples: List[FileInfo]
	affected_fileset_ids: List[int]
	affected_backup_ids: List[int]


class ValidateDbTask(HeavyTask[None]):
	def __init__(self, source: CommandSource, parts: ValidatePart):
		super().__init__(source)
		self.parts = parts
		self.__current_action: Optional[Action] = None

	@property
	@override
	def id(self) -> str:
		return 'db_validate'

	@override
	def is_abort_able(self) -> bool:
		return True

	def __show_bad_objects(self, vlogger: logging.Logger, bad_items: List[_T], item_formatter: Callable[[_T], Union[str, RTextBase]]):
		for i, item in enumerate(bad_items):
			text = RTextBase.format('{}. {}', i + 1, item_formatter(item))
			vlogger.info(text.to_plain_text())
			self.reply(text)

	def __show_affected_file_and_backup(self, result: _ResultWithFileAndBackupSamples, counts: ObjectCounts, vlogger: log_utils.FileLogger):
		vlogger.info('Affected file objects / total file objects: {} / {}'.format(result.affected_file_count, counts.file_object_count))
		vlogger.info('Affected file samples (len={}):'.format(len(result.affected_file_samples)))
		for file in result.affected_file_samples:
			vlogger.info('- {!r}'.format(file))
		vlogger.info('Affected backup / total backups: {} / {}'.format(len(result.affected_backup_ids), counts.backup_count))
		vlogger.info('Affected backup IDs (bad blobs): {}'.format(result.affected_backup_ids))

		sampled_backup_ids = result.affected_backup_ids[:100]
		sampled_fileset_ids = result.affected_fileset_ids[:100]
		self.reply_tr(
			'validate_blobs.affected',

			TextComponents.number(result.affected_file_count),
			TextComponents.number(counts.file_object_count),

			TextComponents.number(len(result.affected_fileset_ids)).
			h(TextComponents.fileset_id_list(sampled_fileset_ids)).
			c(RAction.copy_to_clipboard, ', '.join(map(str, sampled_fileset_ids))),
			TextComponents.number(counts.fileset_count),

			TextComponents.number(len(result.affected_backup_ids)).
			h(TextComponents.backup_id_list(sampled_backup_ids)).
			c(RAction.copy_to_clipboard, ', '.join(map(str, sampled_backup_ids))),
			TextComponents.number(counts.backup_count),
		)
		self.reply_tr('validate_blobs.see_log', str(vlogger.log_file))

	def __validate_blobs(self, vlogger: log_utils.FileLogger) -> bool:
		result = self.run_action(ValidateBlobsAction())

		vlogger.info('Validate blobs result: total={} validated={} ok={}'.format(result.total, result.validated, result.ok))
		self.reply_tr('validate_blobs.done', TextComponents.number(result.validated), TextComponents.number(result.total))
		if result.ok == result.validated:
			self.reply(self.tr('validate_blobs.all_ok', TextComponents.number(result.validated)).set_color(RColor.green))
			return True

		self.reply(self.tr('validate_blobs.found_bad_blobs', TextComponents.number(result.bad), TextComponents.number(result.validated)).set_color(RColor.red))
		for bad_type, bad_items in result.group_bad_by_type().items():
			def item_formatter(item: BadBlobItem) -> str:
				return f'{item.blob.hash}: {item.desc}'
			vlogger.info('bad blob with category "{}" (len={})'.format(bad_type, len(bad_items)))
			self.reply_tr(f'validate_blobs.bad_type.{bad_type.name}', TextComponents.number(len(bad_items)))
			self.__show_bad_objects(vlogger, bad_items, item_formatter)

		counts = GetObjectCountsAction().run()
		self.__show_affected_file_and_backup(result, counts, vlogger)
		return False

	def __validate_chunks(self, result: ValidateChunksResult, vlogger: logging.Logger) -> Tuple[bool, int]:
		vlogger.info('Validate chunks result: total={} validated={} ok={}'.format(result.total, result.validated, result.ok))
		if result.ok == result.validated:
			return True, result.validated

		self.reply(self.tr('validate_chunks.chunk.found_bad_chunks', TextComponents.number(result.bad), TextComponents.number(result.validated)).set_color(RColor.red))
		for bad_type, bad_items in result.group_bad_by_type().items():
			def item_formatter(item: BadChunkItem) -> str:
				return f'id={item.chunk.id} hash={item.chunk.hash}: {item.desc}'
			vlogger.info('bad chunk with category {!r} (len={})'.format(bad_type.name, len(bad_items)))
			self.reply_tr(f'validate_chunks.chunk.bad_type.{bad_type.name}', TextComponents.number(len(bad_items)))
			self.__show_bad_objects(vlogger, bad_items, item_formatter)
		return False, result.validated

	def __validate_chunk_groups(self, result: ValidateChunkGroupsResult, vlogger: logging.Logger) -> Tuple[bool, int]:
		vlogger.info('Validate chunk groups result: total={} validated={} ok={}'.format(result.total, result.validated, result.ok))
		if result.ok == result.validated:
			return True, result.validated

		self.reply(self.tr('validate_chunks.chunk_group.found_bad_chunk_groups', TextComponents.number(result.bad), TextComponents.number(result.validated)).set_color(RColor.red))
		for bad_type, bad_items in result.group_bad_by_type().items():
			def item_formatter(item: BadChunkGroupItem) -> str:
				return f'id={item.chunk_group.id} hash={item.chunk_group.hash}: {item.desc}'
			vlogger.info('bad chunk group with category {!r} (len={})'.format(bad_type.name, len(bad_items)))
			self.reply_tr(f'validate_chunks.chunk_group.bad_type.{bad_type.name}', TextComponents.number(len(bad_items)))
			self.__show_bad_objects(vlogger, bad_items, item_formatter)
		return False, result.validated

	def __validate_chunk_relation_bindings(self, result_cgc: ValidateChunkGroupChunkBindingsResult, result_bcg: ValidateBlobChunkGroupBindingsResult, vlogger: logging.Logger) -> Tuple[bool, int, int]:
		vlogger.info('Validate chunk relation bindings result: ChunkGroupChunkBinding total={} ok={}, BlobChunkGroupBinding total={} ok={}'.format(
			result_cgc.total, result_cgc.ok, result_bcg.total, result_bcg.ok,
		))

		def show_chunk_group_chunk_binding():
			self.reply(self.tr('validate_chunks.chunk_group_chunk_binding.found_bad_bindings', TextComponents.number(result_cgc.bad), TextComponents.number(result_cgc.total)).set_color(RColor.red))
			for bad_type, bad_items in result_cgc.group_bad_by_type().items():
				def item_formatter(item: BadChunkGroupChunkBindingItem) -> str:
					return f'chunk_group_id={item.binding.chunk_group_id} chunk_offset={item.binding.chunk_offset}: {item.desc}'
				vlogger.info('bad chunk group binding with category {!r} (len={})'.format(bad_type.name, len(bad_items)))
				self.reply_tr(f'validate_chunks.chunk_group_chunk_binding.bad_type.{bad_type.name}', TextComponents.number(len(bad_items)))
				self.__show_bad_objects(vlogger, bad_items, item_formatter)

		def show_blob_chunk_group_binding():
			self.reply(self.tr('validate_chunks.blob_chunk_group_binding.found_bad_bindings', TextComponents.number(result_bcg.bad), TextComponents.number(result_bcg.total)).set_color(RColor.red))
			for bad_type, bad_items in result_bcg.group_bad_by_type().items():
				def item_formatter(item: BadBlobChunkGroupBindingItem) -> str:
					return f'blob_id={item.binding.blob_id} chunk_group_offset={item.binding.chunk_group_offset}: {item.desc}'
				vlogger.info('bad blob chunk group binding with category {!r} (len={})'.format(bad_type.name, len(bad_items)))
				self.reply_tr(f'validate_chunks.blob_chunk_group_binding.bad_type.{bad_type.name}', TextComponents.number(len(bad_items)))
				self.__show_bad_objects(vlogger, bad_items, item_formatter)

		if result_cgc.bad > 0:
			show_chunk_group_chunk_binding()
		if result_bcg.bad > 0:
			show_blob_chunk_group_binding()
		ok = result_cgc.ok == result_cgc.total and result_bcg.ok == result_bcg.total
		return ok, result_cgc.total, result_bcg.total

	def __validate_chunk_objects(self, vlogger: log_utils.FileLogger) -> bool:
		all_result = self.run_action(ValidateChunkObjectsAction())
		chunk_ok, chunk_cnt = self.__validate_chunks(all_result.chunk_result, vlogger)
		chunk_group_ok, chunk_group_cnt = self.__validate_chunk_groups(all_result.chunk_group_result, vlogger)
		binding_ok, chunk_group_chunk_binding_cnt, blob_chunk_group_binding_cnt = self.__validate_chunk_relation_bindings(all_result.chunk_group_chunk_bindings_result, all_result.blob_chunk_group_bindings_result,  vlogger)
		if chunk_ok and chunk_group_ok and binding_ok:
			self.reply(self.tr('validate_chunks.all_ok', *[TextComponents.number(num) for num in [chunk_cnt, chunk_group_cnt, chunk_group_chunk_binding_cnt, blob_chunk_group_binding_cnt]]).set_color(RColor.green))
			return True
		else:
			counts = GetObjectCountsAction().run()
			vlogger.info('Affected blob objects / total blob objects: {} / {}'.format(all_result.affected_blob_count, counts.blob_count))
			vlogger.info('Affected blob samples (len={}):'.format(len(all_result.affected_blob_samples)))
			for blob in all_result.affected_blob_samples:
				vlogger.info('- {}'.format(blob.hash))
			self.__show_affected_file_and_backup(all_result, counts, vlogger)
		return False

	def __validate_files(self, vlogger: logging.Logger) -> bool:
		result = self.run_action(ValidateFilesAction())

		vlogger.info('Validate files result: total={} validated={} ok={}'.format(result.total, result.validated, result.ok))
		self.reply_tr('validate_files.done', TextComponents.number(result.validated), TextComponents.number(result.total))
		if result.ok == result.validated:
			self.reply(self.tr('validate_files.all_ok', TextComponents.number(result.validated)).set_color(RColor.green))
			return True

		def show(what: str, lst: List[Tuple[FileInfo, str]]):
			if len(lst) == 0:
				return
			vlogger.info('bad file with category {} (len={})'.format(what, len(lst)))
			self.reply_tr(f'validate_files.bad_type.{what}', TextComponents.number(len(lst)))

			def item_formatter(item: Tuple[FileInfo, str]) -> RTextBase:
				file, msg = item
				return RTextBase.format('fileset={} path={!r}: {}', TextComponents.fileset_id(file.fileset_id), file.path, msg)

			self.__show_bad_objects(vlogger, lst, item_formatter)

		self.reply(self.tr('validate_files.found_bad_files', TextComponents.number(result.bad), TextComponents.number(result.validated)).set_color(RColor.red))
		for bfit in BadFileItemType:
			show(bfit.name, result.get_bad_by_type(bfit))

		return False

	def __validate_filesets(self, vlogger: logging.Logger) -> bool:
		result = self.run_action(ValidateFilesetsAction())
		vlogger.info('Validate filesets result: total={} validated={} ok={}'.format(result.total, result.validated, result.ok))
		self.reply_tr('validate_filesets.done', TextComponents.number(result.validated), TextComponents.number(result.total))
		if result.ok == result.validated:
			self.reply(self.tr('validate_filesets.all_ok', TextComponents.number(result.validated)).set_color(RColor.green))
			return True

		def show(what: str, lst: List[Tuple[FilesetInfo, str]]):
			if len(lst) == 0:
				return
			vlogger.info('bad fileset with category {} (len={})'.format(what, len(lst)))
			self.reply_tr(f'validate_filesets.bad_type.{what}', TextComponents.number(len(lst)))

			def item_formatter(item: Tuple[FilesetInfo, str]) -> RTextBase:
				fileset, msg = item
				return RTextBase.format('{}: {}', TextComponents.fileset_id(fileset.id), msg)

			self.__show_bad_objects(vlogger, lst, item_formatter)

		self.reply(self.tr('validate_filesets.found_bad_filesets', TextComponents.number(result.bad), TextComponents.number(result.validated)).set_color(RColor.red))
		for it in BadFilesetItemType:
			show(it.name, result.get_bad_by_type(it))
		if result.bad == len(result.get_bad_by_type(BadFilesetItemType.orphan)):
			# orphan only, fixable with `!!pb database prune`
			self.reply(self.tr('validate_filesets.fix_orphan_tip', TextComponents.command('database prune', suggest=True)).set_color(RColor.gold))

		vlogger.info('Affected backup IDs (bad filesets): {}'.format(result.affected_backup_ids))
		return False

	def __validate_backups(self, vlogger: logging.Logger) -> bool:
		result = self.run_action(ValidateBackupsAction())
		vlogger.info('Validate backups result: total={} validated={} ok={}'.format(result.total, result.validated, result.ok))
		self.reply_tr('validate_backups.done', TextComponents.number(result.validated), TextComponents.number(result.total))
		if result.ok == result.validated:
			self.reply(self.tr('validate_backups.all_ok', TextComponents.number(result.validated)).set_color(RColor.green))
			return True

		def item_formatter(item: BadBackupItem) -> RTextBase:
			return RTextBase.format('{}: {}', TextComponents.backup_id(item.backup.id), item.desc)
		self.reply(self.tr('validate_backups.found_bad_backups', TextComponents.number(result.bad), TextComponents.number(result.validated)).set_color(RColor.red))
		self.__show_bad_objects(vlogger, result.bad_backups, item_formatter)
		return False

	@override
	def run(self) -> None:
		if not self.parts:
			self.reply_tr('nothing_to_validate')
			return

		t = time.time()
		with log_utils.open_file_logger('validate') as validate_logger:
			validate_logger.info('Validation start, parts: {}'.format(self.parts))

			validators: Dict[ValidatePart, Callable[[log_utils.FileLogger], bool]] = {
				ValidatePart.blobs: self.__validate_blobs,
				ValidatePart.chunks: self.__validate_chunk_objects,
				ValidatePart.files: self.__validate_files,
				ValidatePart.filesets: self.__validate_filesets,
				ValidatePart.backups: self.__validate_backups,
			}
			validate_result: Dict[ValidatePart, bool] = {}
			for part, func in validators.items():
				if part in self.parts and not self.aborted_event.is_set():
					self.reply_tr(f'validate_{part.name}')
					validate_result[part] = func(validate_logger)

		t_cost = TextComponents.number(f'{time.time() - t:.2f}s')
		t_summary = RTextBase.join(', ', [
			RTextList(self.tr(f'part.{part.name}'), ': ', self.tr('result.good').set_color(RColor.green) if res else self.tr('result.bad').set_color(RColor.red))
			for part, res in validate_result.items()
		])
		self.reply_tr('done', t_cost, t_summary)
