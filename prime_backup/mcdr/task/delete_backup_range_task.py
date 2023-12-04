import contextlib
import threading
from typing import Optional, Iterable

from mcdreforged.api.all import *

from prime_backup.action.delete_backup_action import DeleteBackupAction
from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.action.list_backup_action import ListBackupIdAction
from prime_backup.exceptions import BackupNotFound
from prime_backup.mcdr.task import OperationTask, TaskEvent
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.types.blob_info import BlobListSummary
from prime_backup.utils.waitable_value import WaitableValue


class DeleteBackupRangeTask(OperationTask):
	def __init__(self, source: CommandSource, id_start: Optional[int], id_end: Optional[int]):
		super().__init__(source)
		self.id_start = id_start
		self.id_end = id_end
		self.is_confirmed: WaitableValue[bool] = WaitableValue()
		self.is_aborted = threading.Event()

	@property
	def name(self) -> str:
		return 'delete_range'

	def is_abort_able(self) -> bool:
		return True

	def __reply_backups(self, backup_ids: Iterable[int]):
		for backup_id in backup_ids:
			with contextlib.suppress(BackupNotFound):
				backup = GetBackupAction(backup_id, calc_size=False).run()
				self.reply(TextComponents.backup_full(backup, operation_buttons=False, show_size=False))

	def run(self) -> None:
		backup_filter = BackupFilter()
		backup_filter.id_start = self.id_start
		backup_filter.id_end = self.id_end
		backup_ids = ListBackupIdAction(backup_filter=backup_filter).run()
		if len(backup_ids) == 0:
			self.reply(self.tr('no_backup'))

		self.reply(self.tr('to_delete_count', TextComponents.number(len(backup_ids))))
		n = 10
		if len(backup_ids) <= n:
			self.__reply_backups(backup_ids)
		else:
			self.__reply_backups(backup_ids[:n // 2])
			self.reply(RText('...', RColor.gray).h(self.tr('ellipsis.hover', TextComponents.number(len(backup_ids) - n))))
			self.__reply_backups(backup_ids[-n // 2:])

		confirm_time_wait = self.config.command.confirm_time_wait
		self.reply(TextComponents.confirm_hint(self.tr('confirm_target'), TextComponents.duration(confirm_time_wait)))
		self.is_confirmed.wait(confirm_time_wait.value)

		if not self.is_confirmed.is_set():
			self.reply(self.tr('no_confirm'))
			return
		elif self.is_confirmed.get() is False:
			self.reply(self.tr('aborted'))
			return

		cnt = 0
		bls = BlobListSummary.zero()
		for backup_id in backup_ids:
			if self.is_aborted.is_set():
				self.reply(self.tr('aborted'))
				return
			try:
				dr = DeleteBackupAction(backup_id).run()
			except BackupNotFound:
				pass
			else:
				cnt += 1
				bls = bls + dr.bls
				self.reply(self.tr('deleted', TextComponents.backup_brief(dr.backup, backup_id_fancy=False)))
		self.reply(self.tr('done', TextComponents.number(cnt), TextComponents.blob_list_summary_store_size(bls)))

	def on_event(self, event: TaskEvent):
		if event in [TaskEvent.plugin_unload, TaskEvent.operation_aborted]:
			self.is_confirmed.set(False)
			self.is_aborted.set()
		elif event == TaskEvent.operation_confirmed:
			self.is_confirmed.set(True)
