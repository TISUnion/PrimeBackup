from typing import Optional

from typing_extensions import override

from prime_backup.mcdr.task.backup.create_backup_task import CreateBackupTask
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.mcdr.text_components import TextComponents


class CreateScheduledBackupTask(HeavyTask[Optional[int]]):
	def __init__(self, task: CreateBackupTask):
		super().__init__(task.source)
		self.task = task

	@property
	@override
	def id(self) -> str:
		return 'backup_create_scheduled'

	@override
	def run(self) -> Optional[int]:
		if self.config.scheduled_backup.pre_backup_notice:
			notice_delay = self.config.scheduled_backup.pre_backup_notice_delay
			self.broadcast(self.tr('notice', TextComponents.duration(notice_delay)))
			if self.aborted_event.wait(notice_delay.value):
				return None
		else:
			self.broadcast(self.tr('start'))
		return self.run_subtask(self.task)
