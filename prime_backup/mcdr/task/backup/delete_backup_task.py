from abc import ABC
from typing import List, Optional

from mcdreforged.api.all import *
from typing_extensions import override

from prime_backup.action.delete_backup_action import DeleteBackupAction
from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.action.list_backup_action import ListBackupAction
from prime_backup.exceptions import BackupNotFound
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.blob_info import BlobListSummary
from prime_backup.utils import collection_utils
from prime_backup.utils.mcdr_utils import TranslationContext


class _DeleteBackupTaskBase(HeavyTask[None], ABC):
	__base_tr = TranslationContext('task.backup_delete_base').tr

	@override
	def is_abort_able(self) -> bool:
		return True

	def _reply_backups(self, backups: List[BackupInfo]):
		for backup in backups:
			self.reply(TextComponents.backup_full(backup, operation_buttons=False))

	def _show_backups(self, backups: List[BackupInfo], *, max_display: int = 10):
		self.reply(self.__base_tr('to_delete_count', TextComponents.number(len(backups))))
		if len(backups) <= max_display:
			self._reply_backups(backups)
		else:
			self._reply_backups(backups[:max_display // 2])
			self.reply(RText('...', RColor.gray).h(self.__base_tr('ellipsis.hover', TextComponents.number(len(backups) - max_display))))
			self._reply_backups(backups[-max_display // 2:])

	def _wait_confirm(self) -> bool:
		return self.wait_confirm(self.__base_tr('confirm_target'))

	def _do_delete(self, backups: List[BackupInfo], show_start_end: bool):
		if show_start_end:
			self.reply(self.__base_tr('start', TextComponents.number(len(backups))))

		cnt = 0
		bls = BlobListSummary.zero()
		for backup in backups:
			if self.aborted_event.is_set():
				self.reply(self.get_aborted_text())
				break

			try:
				dr = DeleteBackupAction(backup.id).run()
			except BackupNotFound:
				self.reply(self.__base_tr('deleted.skipped', TextComponents.backup_id(backup.id)))
			else:
				cnt += 1
				bls += dr.bls
				self.reply(self.__base_tr('deleted', TextComponents.backup_brief(dr.backup, backup_id_fancy=False)))

		if show_start_end:
			self.reply(self.__base_tr('done', TextComponents.number(cnt), TextComponents.blob_list_summary_store_size(bls)))


class DeleteBackupTask(_DeleteBackupTaskBase):
	def __init__(self, source: CommandSource, backup_ids: List[int], needs_confirm: bool):
		super().__init__(source)
		self.backup_ids = collection_utils.deduplicated_list(backup_ids)
		self.needs_confirm = needs_confirm
		if len(self.backup_ids) == 0:
			raise ValueError()

	@property
	@override
	def id(self) -> str:
		return 'backup_delete'

	@override
	def run(self):
		backups: List[BackupInfo] = []
		for backup_id in self.backup_ids:
			backup = GetBackupAction(backup_id).run()
			if backup.tags.is_protected():
				self.reply_tr('protected', TextComponents.backup_id(backup.id))
				return
			backups.append(backup)

		if len(backups) == 0:
			self.reply_tr('no_backup')
			return

		self._show_backups(backups)
		if self.needs_confirm and not self._wait_confirm():
			return

		self._do_delete(backups, show_start_end=len(backups) > 1)


class DeleteBackupRangeTask(_DeleteBackupTaskBase):
	def __init__(self, source: CommandSource, id_start: Optional[int], id_end: Optional[int]):
		super().__init__(source)
		self.id_start = id_start
		self.id_end = id_end

	@property
	@override
	def id(self) -> str:
		return 'backup_delete_range'

	@override
	def run(self) -> None:
		backup_filter = BackupFilter()
		backup_filter.id_start = self.id_start
		backup_filter.id_end = self.id_end
		backup_filter.requires_non_protected_backup()
		backups = ListBackupAction(backup_filter=backup_filter).run()
		backups = [backup for backup in backups if not backup.tags.is_protected()]  # double check
		if len(backups) == 0:
			self.reply_tr('no_backup')
			return

		self._show_backups(backups)
		if not self._wait_confirm():
			return
		self._do_delete(backups, show_start_end=True)
