import copy

from mcdreforged.api.all import *

from prime_backup.action.count_backup_action import CountBackupAction
from prime_backup.action.list_backup_action import ListBackupAction
from prime_backup.mcdr.task import ReaderTask
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.utils.mcdr_utils import Texts, mkcmd


class ListBackupTask(ReaderTask):
	def __init__(self, source: CommandSource, per_page: int, page: int, backup_filter: BackupFilter):
		super().__init__(source)
		self.source = source
		self.backup_filter = copy.copy(backup_filter)
		self.per_page = per_page
		self.page = page

	@property
	def name(self) -> str:
		return 'list'

	def run(self):
		total_count = CountBackupAction(self.backup_filter).run()
		backups = ListBackupAction(self.backup_filter, self.per_page, (self.page - 1) * self.per_page).run()

		self.reply(RTextList(RText('======== ', RColor.gray), self.tr('title', total_count), RText(' ========', RColor.gray)))
		for backup in backups:
			bid = Texts.backup_id(backup.id, hover=False).h(self.tr('hover.id', backup.id))
			self.reply(RTextList(
				'[', bid, '] ',
				RText('[>]', color=RColor.green).h(self.tr('hover.restore', bid)).c(RAction.suggest_command, mkcmd(f'back {backup.id}')), ' ',
				RText('[x]', color=RColor.red).h(self.tr('hover.delete', bid)).c(RAction.suggest_command, mkcmd(f'delete {backup.id}')), ' ',
				Texts.file_size(backup.size).h(self.tr('hover.size')), ' ',
				self.tr('date'), ': ', backup.date, '; ',
				self.tr('comment'), ': ', Texts.backup_comment(backup.comment).h(self.tr('hover.author', Texts.operator(backup.author))),
			))

		max_page = max(0, (total_count - 1) // self.per_page + 1)
		t_prev = RText('<-')
		if 1 <= self.page - 1 <= max_page:  # has prev
			t_prev.h(self.tr('prev')).c(RAction.run_command, mkcmd(f'list {self.page - 1} {self.per_page}'))
		else:
			t_prev.set_color(RColor.dark_gray)
		t_next = RText('->')
		if 1 <= self.page + 1 <= max_page:  # has next
			t_next.h(self.tr('next')).c(RAction.run_command, mkcmd(f'list {self.page + 1} {self.per_page}'))
		else:
			t_next.set_color(RColor.dark_gray)
		self.reply(RTextList(t_prev, ' ', RText(self.page, RColor.yellow), '/', RText(max_page, RColor.yellow), ' ', t_next))

