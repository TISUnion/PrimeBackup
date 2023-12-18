import json

from mcdreforged.api.all import CommandSource

from prime_backup.action.rename_backup_action import RenameBackupAction
from prime_backup.mcdr.task.basic_task import LightTask
from prime_backup.mcdr.text_components import TextComponents


class RenameBackupTask(LightTask[None]):
	def __init__(self, source: CommandSource, backup_id: int, comment: str):
		super().__init__(source)
		self.backup_id = backup_id
		self.comment = comment

	@property
	def id(self) -> str:
		return 'backup_rename'

	def run(self) -> None:
		RenameBackupAction(self.backup_id, self.comment).run()
		self.reply_tr('modified', TextComponents.backup_id(self.backup_id), json.dumps(self.comment, ensure_ascii=False))
