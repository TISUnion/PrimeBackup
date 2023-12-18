from mcdreforged.api.all import *

from prime_backup.mcdr.crontab_job.basic_job import BasicCrontabJob
from prime_backup.mcdr.task.crontab import CrontabTaskBase
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.utils.mcdr_utils import mkcmd


class ShowCrontabJobTask(CrontabTaskBase[None]):
	@property
	def id(self) -> str:
		return 'crontab_show'

	def run(self) -> None:
		job = self.get_job()

		self.reply(TextComponents.title(self.tr('title', job.get_name_text())))
		self.reply_tr('enabled', TextComponents.boolean(job.is_enabled()))

		if not job.is_enabled():
			return

		self.reply_tr('running', TextComponents.boolean(not job.is_pause()))
		if isinstance(job, BasicCrontabJob):
			if job.interval is not None:
				self.reply_tr('interval', TextComponents.duration(job.interval))
			elif job.crontab is not None:
				self.reply_tr('crontab', TextComponents.crontab(job.crontab))
			else:
				# should never come here cuz there's the config validation
				self.reply(RText('ERROR: no valid trigger', RColor.red))
			self.reply_tr('jitter', TextComponents.duration(job.jitter))
		self.reply_tr('next_run_date', job.get_next_run_date())

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
		self.reply_tr('quick_actions', RTextBase.join(' ', buttons))
