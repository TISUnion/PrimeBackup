from mcdreforged.api.all import *

from prime_backup.action.delete_backup_action import DeleteBackupAction
from prime_backup.mcdr.task import OperationTask
from prime_backup.utils.mcdr_utils import Texts


class DeleteBackupTask(OperationTask):
	def __init__(self, source: CommandSource, backup_id: int):
		super().__init__(source)
		self.backup_id = backup_id

	@property
	def name(self) -> str:
		return 'delete'

	def run(self):
		self.reply(self.tr('deleting', Texts.backup_id(self.backup_id, hover=False, click=False)))
		backup = DeleteBackupAction(self.backup_id).run()
		# TODO: show freed spaced
		self.reply(self.tr('deleted', Texts.backup_id(backup.id, hover=False, click=False)))
