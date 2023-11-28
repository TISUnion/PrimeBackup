from mcdreforged.api.all import *

from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.mcdr.task import ReaderTask
from prime_backup.utils.mcdr_utils import Texts


class ShowBackupTask(ReaderTask):
	def __init__(self, source: CommandSource, backup_id: int):
		super().__init__(source)
		self.backup_id = backup_id

	@property
	def name(self) -> str:
		return 'show'

	def run(self):
		backup = GetBackupAction(self.backup_id).run()
		self.reply(Texts.title(self.tr('title', Texts.backup_id(backup.id))))
		self.reply(self.tr('date', backup.date))
		self.reply(self.tr('comment', Texts.backup_comment(backup.comment)))
		self.reply(self.tr('size', Texts.file_size(backup.size)))
		self.reply(self.tr('author', Texts.operator(backup.author)))
		if backup.hidden:
			self.reply(self.tr('hidden', backup.hidden))
