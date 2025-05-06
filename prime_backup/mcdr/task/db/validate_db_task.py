import enum
import logging
import time
from typing import List, Optional, TypeVar, Tuple, Callable, Dict

from mcdreforged.api.all import CommandSource, RTextBase, RTextList, RColor, RAction
from typing_extensions import override

from prime_backup.action import Action
from prime_backup.action.get_object_counts_action import GetObjectCountsAction
from prime_backup.action.validate_backups_action import ValidateBackupsAction
from prime_backup.action.validate_blobs_action import ValidateBlobsAction, BadBlobItem
from prime_backup.action.validate_files_action import ValidateFilesAction, BadFileItemType
from prime_backup.action.validate_filesets_action import ValidateFilesetsAction, BadFilesetItemType
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.file_info import FileInfo
from prime_backup.types.fileset_info import FilesetInfo
from prime_backup.utils import log_utils


class ValidatePart(enum.Flag):
	blobs = enum.auto()
	files = enum.auto()
	filesets = enum.auto()
	backups = enum.auto()

	@classmethod
	def all(cls) -> 'ValidatePart':
		flags = ValidatePart(0)
		for flag in ValidatePart:
			flags |= flag
		return flags


_Action = TypeVar('_Action', bound=Action)


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

	def __validate_blobs(self, vlogger: log_utils.FileLogger) -> bool:
		result = self.run_action(ValidateBlobsAction())

		vlogger.info('Validate blobs result: total={} validated={} ok={}'.format(result.total, result.validated, result.ok))
		self.reply_tr('validate_blobs.done', TextComponents.number(result.validated), TextComponents.number(result.total))
		if result.ok == result.validated:
			self.reply(self.tr('validate_blobs.all_ok', TextComponents.number(result.validated)).set_color(RColor.green))
			return True

		def show(what: str, lst: List[BadBlobItem]):
			if len(lst) > 0:
				vlogger.info('bad blob with category "{}" (len={})'.format(what, len(lst)))
				self.reply_tr(f'validate_blobs.{what}', TextComponents.number(len(lst)))
				item: BadBlobItem
				for i, item in enumerate(lst):
					text = RTextBase.format('{}. {}: {}', i + 1, item.blob.hash, item.desc)
					vlogger.info(text.to_plain_text())
					self.reply(text)

		self.reply(self.tr('validate_blobs.found_bad_blobs', TextComponents.number(result.bad), TextComponents.number(result.validated)).set_color(RColor.red))
		show('invalid', result.invalid)
		show('missing', result.missing)
		show('corrupted', result.corrupted)
		show('mismatched', result.mismatched)
		show('orphan', result.orphan)

		counts = GetObjectCountsAction().run()

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
			for i, item in enumerate(lst):
				file, msg = item
				text = RTextBase.format('{}. fileset={} path={!r}: {}', i + 1, TextComponents.fileset_id(file.fileset_id), file.path, msg)
				vlogger.info('%s. %s', i + 1, text.to_plain_text())
				self.reply(text)

		self.reply(self.tr('validate_blobs.found_bad_files', TextComponents.number(result.bad), TextComponents.number(result.validated)).set_color(RColor.red))
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
			for i, item in enumerate(lst):
				fileset, msg = item
				text = RTextBase.format('{}. {}: {}', i + 1, TextComponents.fileset_id(fileset.id), msg)
				vlogger.info(text.to_plain_text())
				self.reply(text)

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

		self.reply(self.tr('validate_backups.found_bad_backups', TextComponents.number(result.bad), TextComponents.number(result.validated)).set_color(RColor.red))
		for i, item in enumerate(result.bad_backups):
			text = RTextBase.format('{}. {}: {}', i + 1, TextComponents.backup_id(item.backup.id), item.desc)
			vlogger.info(text.to_plain_text())
			self.reply(text)
		return False

	@override
	def run(self) -> None:
		if not self.parts:
			self.reply_tr('nothing_to_validate')
			return

		t = time.time()
		with log_utils.open_file_logger('validate') as validate_logger:
			validate_logger.info('Validation start, parts: {}'.format(self.parts))

			validators: Dict[ValidatePart, Callable[[logging.Logger], bool]] = {
				ValidatePart.blobs: self.__validate_blobs,
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
