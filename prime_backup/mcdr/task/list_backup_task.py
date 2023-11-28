import copy

from mcdreforged.api.all import *

from prime_backup.action.count_backup_action import CountBackupAction
from prime_backup.action.list_backup_action import ListBackupAction
from prime_backup.mcdr.task import Task, TaskType
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.utils.mcdr_utils import Elements, mkcmd


class ListBackupTask(Task):
	def __init__(self, source: CommandSource, per_page: int, page: int, backup_filter: BackupFilter):
		super().__init__(source)
		self.source = source
		self.backup_filter = copy.copy(backup_filter)
		self.per_page = per_page
		self.page = page

	@property
	def name(self) -> str:
		return 'list'

	def type(self) -> TaskType:
		return TaskType.read

	def run(self):
		total_count = CountBackupAction(self.backup_filter).run()
		backups = ListBackupAction(self.backup_filter, self.per_page, (self.page - 1) * self.per_page).run()

		self.reply(RTextList(RText('======== ', RColor.gray), self.tr('title', total_count), RText(' ========', RColor.gray)))
		for backup in backups:
			bid = Elements.backup_id(backup.id)
			self.reply(RTextList(
				'[', bid, '] ',
				RText('[I]').h(self.tr('button.inspect', bid)).c(RAction.run_command, mkcmd(f'inspect {backup.id}')), ' ',
				RText('[>]', color=RColor.green).h(self.tr('button.restore', bid)).c(RAction.suggest_command, mkcmd(f'back {backup.id}')),' ',
				RText('[x]', color=RColor.red).h(self.tr('button.delete', bid)).c(RAction.suggest_command, mkcmd(f'delete {backup.id}')), ' ',
				Elements.file_size(backup.size),
				self.tr('date'), ': ', backup.date, '; ',
				self.tr('comment'), ': ', backup.comment,
			))

		max_page = max(0, (total_count - 1) // self.per_page + 1)
		t_prev = RText('<-')
		if 1 <= self.page - 1 <= max_page:  # has prev
			t_prev.h(self.tr('prev')).c(RAction.run_command, mkcmd(f'list {self.per_page} {self.page - 1}'))
		else:
			t_prev.set_color(RColor.gray)
		t_next = RText('->')
		if 1 <= self.page + 1 <= max_page:  # has next
			t_next.h(self.tr('next')).c(RAction.run_command, mkcmd(f'list {self.per_page} {self.page + 1}'))
		else:
			t_next.set_color(RColor.gray)
		self.reply(RTextList(t_prev, ' ', RText(self.page, RColor.gold), '/', RText(max_page, RColor.gold), ' ', t_next))

