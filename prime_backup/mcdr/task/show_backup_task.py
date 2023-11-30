from mcdreforged.api.all import *

from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.mcdr.task import ReaderTask
from prime_backup.mcdr.text_components import TextComponents


class ShowBackupTask(ReaderTask):
	def __init__(self, source: CommandSource, backup_id: int):
		super().__init__(source)
		self.backup_id = backup_id

	@property
	def name(self) -> str:
		return 'show'

	def run(self):
		backup = GetBackupAction(self.backup_id).run()
		self.reply(TextComponents.title(self.tr('title', TextComponents.backup_id(backup.id))))
		self.reply(self.tr('date', backup.date))
		self.reply(self.tr('comment', TextComponents.backup_comment(backup.comment)))
		self.reply(self.tr('size', TextComponents.file_size(backup.raw_size)))
		self.reply(self.tr('author', TextComponents.operator(backup.author)))
		if backup.hidden:
			self.reply(self.tr('hidden', backup.hidden))
