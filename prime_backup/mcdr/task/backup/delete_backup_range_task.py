from typing import Optional, Iterable

from mcdreforged.api.all import *

from prime_backup.action.delete_backup_action import DeleteBackupAction
from prime_backup.action.list_backup_action import ListBackupAction
from prime_backup.exceptions import BackupNotFound
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.blob_info import BlobListSummary


class DeleteBackupRangeTask(HeavyTask[None]):
	def __init__(self, source: CommandSource, id_start: Optional[int], id_end: Optional[int]):
		super().__init__(source)
		self.id_start = id_start
		self.id_end = id_end

	@property
	def id(self) -> str:
		return 'backup_delete_range'

	def is_abort_able(self) -> bool:
		return True

	def __reply_backups(self, backups: Iterable[BackupInfo]):
		for backup in backups:
			self.reply(TextComponents.backup_full(backup, operation_buttons=False))

	def run(self) -> None:
		backup_filter = BackupFilter()
		backup_filter.id_start = self.id_start
		backup_filter.id_end = self.id_end
		backup_filter.filter_non_protected_backup()
		backups = ListBackupAction(backup_filter=backup_filter).run()
		backups = [backup for backup in backups if not backup.tags.is_protected()]  # double check
		if len(backups) == 0:
			self.reply_tr('no_backup')
			return

		self.reply_tr('to_delete_count', TextComponents.number(len(backups)))
		n = 10
		if len(backups) <= n:
			self.__reply_backups(backups)
		else:
			self.__reply_backups(backups[:n // 2])
			self.reply(RText('...', RColor.gray).h(self.tr('ellipsis.hover', TextComponents.number(len(backups) - n))))
			self.__reply_backups(backups[-n // 2:])

		if not self.wait_confirm(self.tr('confirm_target')):
			return

		cnt = 0
		bls = BlobListSummary.zero()
		for backup in backups:
			if self.aborted_event.is_set():
				self.reply(self.get_aborted_text())
				return
			try:
				dr = DeleteBackupAction(backup.id).run()
			except BackupNotFound:
				pass
			else:
				cnt += 1
				bls = bls + dr.bls
				self.reply_tr('deleted', TextComponents.backup_brief(dr.backup, backup_id_fancy=False))
		self.reply_tr('done', TextComponents.number(cnt), TextComponents.blob_list_summary_store_size(bls))
