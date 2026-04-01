from mcdreforged.api.all import CommandSource, RColor
from typing_extensions import override

from prime_backup.action.count_backup_action import CountBackupAction
from prime_backup.action.list_backup_action import ListBackupIdAction
from prime_backup.action.reassign_backup_id_action import ReassignBackupIdAction
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.backup_filter import BackupFilter, BackupSortOrder


class ReassignBackupIdTask(HeavyTask[None]):
	def __init__(self, source: CommandSource, order: BackupSortOrder = BackupSortOrder.id):
		super().__init__(source)
		self.order = order

	@property
	@override
	def id(self) -> str:
		return 'db_reassign_backup_id'

	@override
	def run(self) -> None:
		count = self.run_action(CountBackupAction())
		if count == 0:
			self.reply_tr('no_backups')
			return

		ids = self.run_action(ListBackupIdAction(backup_filter=BackupFilter(sort_order=BackupSortOrder.id_r), limit=1))
		current_max_id = ids[0]

		self.reply_tr('show_whats_going_on', TextComponents.number(count), TextComponents.number(current_max_id), TextComponents.number(self.order.name))
		self.reply(self.tr('warning').set_color(RColor.red))
		if not self.wait_confirm(self.tr('confirm_target')):
			return

		self.reply_tr('start')
		result = self.run_action(ReassignBackupIdAction(self.order))
		self.reply_tr('done', TextComponents.number(1), TextComponents.number(result.max_id))