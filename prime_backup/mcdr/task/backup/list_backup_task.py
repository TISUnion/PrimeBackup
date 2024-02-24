import copy
import json

from mcdreforged.api.all import *

from prime_backup.action.count_backup_action import CountBackupAction
from prime_backup.action.list_backup_action import ListBackupAction
from prime_backup.mcdr.task.basic_task import LightTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.utils import conversion_utils
from prime_backup.utils.mcdr_utils import mkcmd


class ListBackupTask(LightTask[None]):
	def __init__(self, source: CommandSource, per_page: int, page: int, backup_filter: BackupFilter, show_all: bool, show_flags: bool):
		super().__init__(source)
		self.source = source
		self.backup_filter = copy.copy(backup_filter)
		self.per_page = per_page
		self.page = page
		self.show_all = show_all
		self.show_flags = show_flags

		if not self.show_all:
			self.backup_filter.filter_non_temporary_backup()
			self.backup_filter.filter_non_hidden_backup()

	@property
	def id(self) -> str:
		return 'backup_list'

	def __make_command(self, page: int) -> str:
		def date_str(ts_ns: int) -> str:
			return json.dumps(conversion_utils.timestamp_to_local_date_str(ts_ns, decimal=ts_ns % 1000 != 0), ensure_ascii=False)

		cmd = mkcmd(f'list {page} --per-page {self.per_page}')
		if self.backup_filter.creator is not None:
			cmd += f' --creator {self.backup_filter.creator}'
		if self.backup_filter.timestamp_start is not None:
			cmd += f' --from {date_str(self.backup_filter.timestamp_start)}'
		if self.backup_filter.timestamp_end is not None:
			cmd += f' --to {date_str(self.backup_filter.timestamp_end)}'
		if self.show_all:
			cmd += f' --all'
		if self.show_flags:
			cmd += f' --flags'

		return cmd

	def run(self):
		total_count = CountBackupAction(self.backup_filter).run()
		backups = ListBackupAction(backup_filter=self.backup_filter, limit=self.per_page, offset=(self.page - 1) * self.per_page).run()

		self.reply(TextComponents.title(self.tr('title')))
		self.reply_tr('backup_count', TextComponents.number(total_count))
		for backup in backups:
			self.reply(TextComponents.backup_full(backup, True, show_flags=self.show_flags))

		max_page = max(0, (total_count - 1) // self.per_page + 1)
		t_prev = RText('<-')
		if 1 <= self.page - 1 <= max_page:  # has prev
			t_prev.h(self.tr('prev')).c(RAction.run_command, self.__make_command(self.page - 1))
		else:
			t_prev.set_color(RColor.dark_gray)
		t_next = RText('->')
		if 1 <= self.page + 1 <= max_page:  # has next
			t_next.h(self.tr('next')).c(RAction.run_command, self.__make_command(self.page + 1))
		else:
			t_next.set_color(RColor.dark_gray)
		self.reply(RTextList(t_prev, ' ', TextComponents.number(self.page), '/', TextComponents.number(max_page), ' ', t_next))
