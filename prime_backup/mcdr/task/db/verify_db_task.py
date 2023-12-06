import contextlib
import enum
import time
from typing import List, Optional, ContextManager

from mcdreforged.api.all import *

from prime_backup.action import Action
from prime_backup.action.verify_blobs_action import VerifyBlobsAction, BadBlobItem
from prime_backup.action.verify_files_action import VerifyFilesAction
from prime_backup.mcdr.task import TaskEvent
from prime_backup.mcdr.task.basic_tasks import OperationTask
from prime_backup.mcdr.text_components import TextComponents


class VerifyParts(enum.Flag):
	blobs = enum.auto()
	files = enum.auto()

	@classmethod
	def all(cls) -> 'VerifyParts':
		flags = VerifyParts(0)
		for flag in VerifyParts:
			flags |= flag
		return flags


class VerifyDbTask(OperationTask):
	def __init__(self, source: CommandSource, parts: VerifyParts):
		super().__init__(source)
		self.parts = parts
		self.__current_action: Optional[Action] = None

	@property
	def name(self) -> str:
		return 'db_verify'

	def is_abort_able(self) -> bool:
		return True

	@contextlib.contextmanager
	def __set_action(self, action: Action) -> ContextManager[Action]:
		self.__current_action = action
		if self.aborted_event.is_set():
			action.interrupt()
		try:
			yield action
		finally:
			self.__current_action = None

	def __verify_blobs(self):
		with self.__set_action(VerifyBlobsAction()) as action:
			result = action.run()

		if result.ok == result.total:
			self.reply(self.tr('verify_blobs.all_ok', TextComponents.number(result.total)))
			return

		def show(what: str, lst: List[BadBlobItem]):
			if len(lst) > 0:
				self.reply(self.tr(f'verify_blobs.{what}', TextComponents.number(len(lst))))
				for item in result.missing:
					self.reply(RTextList('{}: {}', item.blob.hash, item.desc))

		self.reply(self.tr('verify_blobs.found_bad_blobs', result.total - result.ok))
		show('invalid', result.invalid)
		show('missing', result.missing)
		show('corrupted', result.corrupted)
		show('mismatched', result.mismatched)

	def __verify_files(self):
		with self.__set_action(VerifyFilesAction()) as action:
			result = action.run()

	def run(self) -> None:
		if not self.parts:
			self.reply(self.tr('nothing_to_verify'))
			return

		t = time.time()

		if VerifyParts.blobs in self.parts and not self.aborted_event.is_set():
			self.reply(self.tr('verify_blobs'))
			self.__verify_blobs()

		if VerifyParts.files in self.parts and not self.aborted_event.is_set():
			self.reply(self.tr('verify_files'))
			self.__verify_files()

		cost = time.time() - t
		self.reply(self.tr('done', TextComponents.number(f'{cost:.2f}s')))

	def on_event(self, event: TaskEvent):
		if (act := self.__current_action) is not None:
			act.interrupt()


