from mcdreforged.api.all import *

from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.mcdr.task import ReaderTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.utils.mcdr_utils import mkcmd


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

		self.reply(self.tr('date', TextComponents.backup_date(backup)))
		self.reply(self.tr('comment', TextComponents.backup_comment(backup.comment)))
		self.reply(self.tr('stored_size', TextComponents.file_size(backup.stored_size), TextComponents.percent(backup.stored_size, backup.raw_size)))
		self.reply(self.tr('raw_size', TextComponents.file_size(backup.raw_size)))

		t_author = TextComponents.operator(backup.author)
		self.reply(self.tr(
			'author',
			t_author.copy().
			h(self.tr('author.hover', t_author.copy())).
			c(RAction.suggest_command, mkcmd(f'list --author {backup.author.name if backup.author.is_player() else str(backup.author)}'))
		))

		if len(backup.tags) > 0:
			self.reply(self.tr('tag.title', len(backup.tags)))
			for k, v in backup.tags.items():
				self.reply(RTextBase.format('  {}: {}', k, v))
		else:
			self.reply(self.tr('tag.empty_title', self.tr('tag.empty').set_color(RColor.gray)))
