import contextlib
import enum
import logging
import time
from typing import List, Optional, ContextManager, TypeVar

from mcdreforged.api.all import *

from prime_backup.action import Action
from prime_backup.action.validate_blobs_action import ValidateBlobsAction, BadBlobItem
from prime_backup.action.validate_files_action import ValidateFilesAction, BadFileItem
from prime_backup.mcdr.task import TaskEvent
from prime_backup.mcdr.task.basic_task import OperationTask
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


class ValidateDbTask(OperationTask):
	def __init__(self, source: CommandSource, parts: ValidateParts):
		super().__init__(source)
		self.parts = parts
		self.__current_action: Optional[Action] = None

	@property
	def name(self) -> str:
		return 'db_validate'

	def is_abort_able(self) -> bool:
		return True

	@contextlib.contextmanager
	def __set_action(self, action: _Action) -> ContextManager[_Action]:
		self.__current_action = action
		if self.aborted_event.is_set():
			action.interrupt()
		try:
			yield action
		finally:
			self.__current_action = None

	def __validate_blobs(self, vlogger: logging.Logger):
		with self.__set_action(ValidateBlobsAction()) as action:
			result = action.run()

		vlogger.info('Validate blobs result: total={} validated={} ok={}'.format(result.total, result.validated, result.ok))
		self.reply(self.tr('validate_blobs.done', TextComponents.number(result.validated), TextComponents.number(result.total)))
		if result.ok == result.validated:
			self.reply(self.tr('validate_blobs.all_ok', TextComponents.number(result.validated)).set_color(RColor.green))
			return

		def show(what: str, lst: List[BadBlobItem]):
			if len(lst) > 0:
				vlogger.info('bad blob with category {} (len={})'.format(what, len(lst)))
				self.reply(self.tr(f'validate_blobs.{what}', TextComponents.number(len(lst))))
				item: BadBlobItem
				for i, item in enumerate(lst):
					text = RTextList('{}: {}', item.blob.hash, item.desc)
					vlogger.info('%s. %s', i + 1, text.to_plain_text())
					self.reply(text)

		self.reply(self.tr('validate_blobs.found_bad_blobs', TextComponents.number(result.validated - result.ok), TextComponents.number(result.validated)).set_color(RColor.red))
		show('invalid', result.invalid)
		show('missing', result.missing)
		show('corrupted', result.corrupted)
		show('mismatched', result.mismatched)

	def __validate_files(self, vlogger: logging.Logger):
		with self.__set_action(ValidateFilesAction()) as action:
			result = action.run()

		vlogger.info('Validate files result: total={} validated={} ok={}'.format(result.total, result.validated, result.ok))
		self.reply(self.tr('validate_files.done', TextComponents.number(result.validated), TextComponents.number(result.total)))
		if result.ok == result.validated:
			self.reply(self.tr('validate_files.all_ok', TextComponents.number(result.validated)).set_color(RColor.green))
			return

		def show(what: str, lst: List[BadFileItem]):
			if len(lst) > 0:
				vlogger.info('bad file with category {} (len={})'.format(what, len(lst)))
				self.reply(self.tr(f'validate_files.{what}', TextComponents.number(len(lst))))
				item: BadFileItem
				for i, item in enumerate(lst):
					text = RTextList('#{} {!r}: {}', item.file.backup_id, item.file.path, item.desc)
					vlogger.info('%s. %s', i + 1, text.to_plain_text())
					self.reply(text)

		self.reply(self.tr('validate_blobs.found_bad_files', TextComponents.number(result.validated - result.ok), TextComponents.number(result.validated)).set_color(RColor.red))
		show('invalid', result.invalid)
		show('bad_blob_relation', result.bad_blob_relation)
		show('file_blob_mismatched', result.file_blob_mismatched)

	def run(self) -> None:
		if not self.parts:
			self.reply(self.tr('nothing_to_validate'))
			return

		t = time.time()
		with log_utils.open_file_logger('validate') as validate_logger:
			validate_logger.info('Validation start, parts: {}'.format(self.parts))

			if ValidateParts.blobs in self.parts and not self.aborted_event.is_set():
				self.reply(self.tr('validate_blobs'))
				self.__validate_blobs(validate_logger)

			if ValidateParts.files in self.parts and not self.aborted_event.is_set():
				self.reply(self.tr('validate_files'))
				self.__validate_files(validate_logger)

		cost = time.time() - t
		self.reply(self.tr('done', TextComponents.number(f'{cost:.2f}s')))

	def on_event(self, event: TaskEvent):
		super().on_event(event)
		if (act := self.__current_action) is not None:
			act.interrupt()
