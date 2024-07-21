import enum
import logging
import time
from typing import List, Optional, TypeVar

from mcdreforged.api.all import *

from prime_backup.action import Action
from prime_backup.action.get_object_counts_action import GetObjectCountsAction
from prime_backup.action.validate_blobs_action import ValidateBlobsAction, BadBlobItem
from prime_backup.action.validate_files_action import ValidateFilesAction, BadFileItem
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.utils import log_utils


class ValidateParts(enum.Flag):
	blobs = enum.auto()
	files = enum.auto()

	@classmethod
	def all(cls) -> 'ValidateParts':
		flags = ValidateParts(0)
		for flag in ValidateParts:
			flags |= flag
		return flags


_Action = TypeVar('_Action', bound=Action)


class ValidateDbTask(HeavyTask[None]):
	def __init__(self, source: CommandSource, parts: ValidateParts):
		super().__init__(source)
		self.parts = parts
		self.__current_action: Optional[Action] = None

	@property
	def id(self) -> str:
		return 'db_validate'

	def is_abort_able(self) -> bool:
		return True

	def __validate_blobs(self, vlogger: log_utils.FileLogger):
		result = self.run_action(ValidateBlobsAction())

		vlogger.info('Validate blobs result: total={} validated={} ok={}'.format(result.total, result.validated, result.ok))
		self.reply_tr('validate_blobs.done', TextComponents.number(result.validated), TextComponents.number(result.total))
		if result.ok == result.validated:
			self.reply(self.tr('validate_blobs.all_ok', TextComponents.number(result.validated)).set_color(RColor.green))
			return

		def show(what: str, lst: List[BadBlobItem]):
			if len(lst) > 0:
				vlogger.info('bad blob with category "{}" (len={})'.format(what, len(lst)))
				self.reply_tr(f'validate_blobs.{what}', TextComponents.number(len(lst)))
				item: BadBlobItem
				for i, item in enumerate(lst):
					text = RTextBase.format('{}. {}: {}', i + 1, item.blob.hash, item.desc)
					vlogger.info(text.to_plain_text())
					self.reply(text)

		self.reply(self.tr('validate_blobs.found_bad_blobs', TextComponents.number(result.validated - result.ok), TextComponents.number(result.validated)).set_color(RColor.red))
		show('invalid', result.invalid)
		show('missing', result.missing)
		show('corrupted', result.corrupted)
		show('mismatched', result.mismatched)
		show('orphan', result.orphan)

		counts = GetObjectCountsAction().run()

		vlogger.info('Affected file / total files: {} / {}'.format(result.affected_file_count, counts.file_count))
		vlogger.info('Affected file samples (len={}):'.format(len(result.affected_file_samples)))
		for file in result.affected_file_samples:
			vlogger.info('- {!r}'.format(file))
		vlogger.info('Affected backup / total backups: {} / {}'.format(len(result.affected_backup_ids), counts.backup_count))
		vlogger.info('Affected backup IDs: {}'.format(result.affected_backup_ids))

		sampled_backup_ids = result.affected_backup_ids[:100]
		self.reply_tr(
			'validate_blobs.affected',
			TextComponents.number(result.affected_file_count),
			TextComponents.number(counts.file_count),
			TextComponents.number(len(result.affected_backup_ids)).
			h(TextComponents.backup_id_list(sampled_backup_ids)).
			c(RAction.copy_to_clipboard, ', '.join(map(str, sampled_backup_ids))),
			TextComponents.number(counts.backup_count),
		)
		self.reply_tr('validate_blobs.see_log', str(vlogger.log_file))

	def __validate_files(self, vlogger: logging.Logger):
		result = self.run_action(ValidateFilesAction())

		vlogger.info('Validate files result: total={} validated={} ok={}'.format(result.total, result.validated, result.ok))
		self.reply_tr('validate_files.done', TextComponents.number(result.validated), TextComponents.number(result.total))
		if result.ok == result.validated:
			self.reply(self.tr('validate_files.all_ok', TextComponents.number(result.validated)).set_color(RColor.green))
			return

		def show(what: str, lst: List[BadFileItem]):
			if len(lst) > 0:
				vlogger.info('bad file with category {} (len={})'.format(what, len(lst)))
				self.reply_tr(f'validate_files.{what}', TextComponents.number(len(lst)))
				item: BadFileItem
				for i, item in enumerate(lst):
					text = RTextBase.format('{}. #{} {!r}: {}', i + 1, item.file.backup_id, item.file.path, item.desc)
					vlogger.info('%s. %s', i + 1, text.to_plain_text())
					self.reply(text)

		self.reply(self.tr('validate_blobs.found_bad_files', TextComponents.number(result.validated - result.ok), TextComponents.number(result.validated)).set_color(RColor.red))
		show('invalid', result.invalid)
		show('bad_blob_relation', result.bad_blob_relation)
		show('file_blob_mismatched', result.file_blob_mismatched)

	def run(self) -> None:
		if not self.parts:
			self.reply_tr('nothing_to_validate')
			return

		t = time.time()
		with log_utils.open_file_logger('validate') as validate_logger:
			validate_logger.info('Validation start, parts: {}'.format(self.parts))

			if ValidateParts.blobs in self.parts and not self.aborted_event.is_set():
				self.reply_tr('validate_blobs')
				self.__validate_blobs(validate_logger)

			if ValidateParts.files in self.parts and not self.aborted_event.is_set():
				self.reply_tr('validate_files')
				self.__validate_files(validate_logger)

		cost = time.time() - t
		self.reply_tr('done', TextComponents.number(f'{cost:.2f}s'))
