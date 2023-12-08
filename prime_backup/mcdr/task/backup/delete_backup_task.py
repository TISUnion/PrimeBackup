from mcdreforged.api.all import *

from prime_backup.action.delete_backup_action import DeleteBackupAction
from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.mcdr.task.basic_task import OperationTask
from prime_backup.mcdr.text_components import TextComponents


class DeleteBackupTask(OperationTask[None]):
	def __init__(self, source: CommandSource, backup_id: int):
		super().__init__(source)
		self.backup_id = backup_id

	@property
	def id(self) -> str:
		return 'backup_delete'

	def run(self):
		backup = GetBackupAction(self.backup_id).run()
		if backup.tags.is_protected():
			self.reply(self.tr('protected', TextComponents.backup_id(backup.id)))
			return

		self.reply(self.tr('deleting', TextComponents.backup_brief(backup, backup_id_fancy=False)))
		dr = DeleteBackupAction(self.backup_id).run()

		self.reply(self.tr(
			'deleted',
			TextComponents.backup_id(self.backup_id, hover=False, click=False),
			TextComponents.blob_list_summary_store_size(dr.bls),
		))
