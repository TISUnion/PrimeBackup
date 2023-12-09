from mcdreforged.api.all import *

from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.mcdr.task.basic_task import ReaderTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.backup_tags import BackupTagName
from prime_backup.types.operator import Operator, PrimeBackupOperatorNames
from prime_backup.utils.mcdr_utils import mkcmd


class ShowBackupTask(ReaderTask[None]):
	def __init__(self, source: CommandSource, backup_id: int):
		super().__init__(source)
		self.backup_id = backup_id

	@property
	def id(self) -> str:
		return 'backup_show'

	def run(self):
		backup = GetBackupAction(self.backup_id).run()

		self.reply(TextComponents.title(self.tr('title', TextComponents.backup_id(backup.id))))

		self.reply(self.tr('date', TextComponents.backup_date(backup)))
		self.reply(self.tr('comment', TextComponents.backup_comment(backup.comment)))
		self.reply(self.tr('stored_size', TextComponents.file_size(backup.stored_size), TextComponents.percent(backup.stored_size, backup.raw_size)))
		self.reply(self.tr('raw_size', TextComponents.file_size(backup.raw_size)))

		t_creator = TextComponents.operator(backup.creator)
		cmd_creator = f'list --creator {backup.creator.name if backup.creator.is_player() else str(backup.creator)}'
		if backup.creator == Operator.pb(PrimeBackupOperatorNames.pre_restore):
			cmd_creator += ' --all'  # pre restore backups are hidden by default
		self.reply(self.tr(
			'creator',
			t_creator.copy().
			h(self.tr('creator.hover', t_creator.copy())).
			c(RAction.suggest_command, mkcmd(cmd_creator))
		))

		if len(backup.tags) > 0:
			self.reply(self.tr('tag.title', TextComponents.number(len(backup.tags))))
			for k, v in backup.tags.items():
				try:
					btn = BackupTagName[k]
				except KeyError:
					pass
				else:
					k = RTextBase.format('{} ({})', btn.value.text.h(btn.name), btn.value.flag)
				self.reply(RTextBase.format('  {}: {}', k, TextComponents.auto(v)))
		else:
			self.reply(self.tr('tag.empty_title', self.tr('tag.empty').set_color(RColor.gray)))
