from mcdreforged.api.all import *

from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.mcdr.task.basic_task import LightTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.backup_tags import BackupTagName
from prime_backup.types.operator import Operator, PrimeBackupOperatorNames
from prime_backup.utils.mcdr_utils import mkcmd


class ShowBackupTask(LightTask[None]):
	def __init__(self, source: CommandSource, backup_id: int):
		super().__init__(source)
		self.backup_id = backup_id

	@property
	def id(self) -> str:
		return 'backup_show'

	def run(self):
		backup = GetBackupAction(self.backup_id).run()

		self.reply(TextComponents.title(self.tr('title', TextComponents.backup_id(backup.id))))

		self.reply_tr('date', TextComponents.backup_date(backup))
		t_comment = self.tr('comment', TextComponents.backup_comment(backup.comment))
		if self.source.has_permission(self.config.command.permission.get('rename')):
			t_comment.h(self.tr('comment_edit', TextComponents.backup_id(backup.id))).c(RAction.suggest_command, mkcmd(f'rename {backup.id} '))
		self.reply(t_comment)
		self.reply_tr('stored_size', TextComponents.file_size(backup.stored_size), TextComponents.percent(backup.stored_size, backup.raw_size))
		self.reply_tr('raw_size', TextComponents.file_size(backup.raw_size))

		t_creator = TextComponents.operator(backup.creator)
		cmd_creator = f'list --creator {backup.creator.name if backup.creator.is_player() else str(backup.creator)}'
		if backup.creator == Operator.pb(PrimeBackupOperatorNames.pre_restore):
			cmd_creator += ' --all'  # pre restore backups are hidden by default
		self.reply_tr(
			'creator',
			t_creator.copy().
			h(self.tr('creator.hover', t_creator.copy())).
			c(RAction.suggest_command, mkcmd(cmd_creator))
		)

		if len(backup.tags) > 0:
			self.reply_tr('tag.title', TextComponents.number(len(backup.tags)))
			for k, v in backup.tags.items():
				try:
					btn = BackupTagName[k]
				except KeyError:
					pass
				else:
					k = RTextBase.format('{} ({})', btn.value.text.h(btn.name), btn.value.flag)
				self.reply(RTextBase.format('  {}: {}', k, TextComponents.auto(v)))
		else:
			self.reply_tr('tag.empty_title', self.tr('tag.empty').set_color(RColor.gray))
