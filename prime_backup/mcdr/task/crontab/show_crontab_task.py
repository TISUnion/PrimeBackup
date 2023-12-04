from mcdreforged.api.all import *

from prime_backup.mcdr.task.crontab import CrontabTaskBase
from prime_backup.mcdr.text_components import TextComponents, TextColors
from prime_backup.utils.mcdr_utils import mkcmd


class ShowCrontabJobTask(CrontabTaskBase):
	@property
	def name(self) -> str:
		return 'crontab_show'

	def run(self) -> None:
		job = self.get_job()
		self.reply(RTextList(
			RText('======== ', RColor.gray),
			self.tr('title', job.get_name_text()),
			RText(' ========', RColor.gray),
		))
		self.reply(self.tr('enabled', TextComponents.boolean(self.config.scheduled_backup.enabled)))
		self.reply(self.tr('running', TextComponents.boolean(not job.is_pause())))
		self.reply(self.tr('interval', TextComponents.duration(job.interval, color=TextColors.number)))
		self.reply(self.tr('jitter', TextComponents.duration(job.jitter, color=TextColors.number)))
		self.reply(self.tr('next_run_date', job.get_next_run_date()))

		def make(op: str, color: RColor) -> RTextBase:
			from prime_backup.mcdr.task.crontab.operate_crontab_task import OperateCrontabJobTask
			_ = OperateCrontabJobTask.Operation[op]
			return (
				RTextList('[', self.tr(op), ']').
				set_color(color).
				h(self.tr(f'{op}.hover', job.get_name_text())).
				c(RAction.run_command, mkcmd(f'crontab {self.job_id.name} {op}'))
			)
		buttons = [
			make('pause', RColor.gold),
			make('resume', RColor.dark_green),
		]
		self.reply(self.tr('quick_actions', RTextBase.join(' ', buttons)))
