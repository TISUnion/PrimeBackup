import copy

from mcdreforged.api.all import *

from prime_backup.action.list_backup_action import ListBackupAction
from prime_backup.config.types import ByteCount
from prime_backup.mcdr.task import Task
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.utils.mcdr_utils import mkcmd


def tr(key: str, *args, **kwargs):
	from prime_backup.utils.mcdr_utils import tr
	return tr('task.list.' + key, *args, **kwargs)


class ListBackupTask(Task):
	def __init__(self, source: CommandSource, limit: int, page: int, backup_filter: BackupFilter):
		super().__init__()
		self.source = source
		self.backup_filter = copy.copy(backup_filter)
		self.limit = limit
		self.page = page

	def run(self):
		backups = ListBackupAction(self.backup_filter, self.limit, (self.page - 1) * self.limit).run()
		if self.backup_filter:
			# Recent 10 backups
			self.source.reply('')
		for backup in backups:
			self.source.reply(RTextList(
				'[',
				RText('#', RColor.gray),
				RText(backup.id, RColor.gold).h(f'ID: {backup.id}').c(RAction.suggest_command, mkcmd(f'inspect {backup.id}')),
				'] ',
				RText(ByteCount(backup.size), color=RColor.yellow),
				tr('date'), ': ', backup.date, '; ',
				tr('comment'), ': ', backup.comment,
			))

