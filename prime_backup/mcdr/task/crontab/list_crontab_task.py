from mcdreforged.api.all import *

from prime_backup.mcdr.crontab_manager import CrontabManager
from prime_backup.mcdr.task.basic_task import ImmediateTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.utils.mcdr_utils import mkcmd


class ListCrontabJobTask(ImmediateTask[None]):
	def __init__(self, source: CommandSource, crontab_manager: CrontabManager):
		super().__init__(source)
		self.crontab_manager = crontab_manager

	@property
	def id(self) -> str:
		return 'crontab_list_job'

	def run(self) -> None:
		self.reply(TextComponents.title(self.tr('title')))
		for job in self.crontab_manager.list_jobs():
			if job.is_enabled():
				t_enabled = RText('E', RColor.dark_green).h(self.tr('enabled'))
				if job.is_running():
					t_running = RText('R', RColor.dark_green).h(self.tr('running'))
				else:
					t_running = RText('P', RColor.yellow).h(self.tr('paused'))
			else:
				t_enabled = RText('D', RColor.dark_red).h(self.tr('disabled'))
				t_running = RText('-', RColor.dark_gray).h(self.tr('disabled'))

			t_flags = RTextList(
				RText('[', RColor.gray),
				RTextBase.join(RText(',', RColor.dark_gray), [t_enabled, t_running]),
				RText(']', RColor.gray),
			)

			nt = job.get_name_text().c(RAction.suggest_command, mkcmd(f'crontab {job.id.name} '))
			if job.is_running():
				self.reply_tr('line.running', nt, t_flags, job.get_duration_until_next_run_text(), job.get_next_run_date())
			else:
				self.reply_tr('line.paused', nt, t_flags)
