import threading
from typing import Dict

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.blocking import BlockingScheduler

from prime_backup import constants, logger
from prime_backup.mcdr.crontab_job import CrontabJob, CrontabJobId, CrontabJobEvent
from prime_backup.mcdr.crontab_job.prune_backup_job import PruneBackupJob
from prime_backup.mcdr.crontab_job.scheduled_backup_job import ScheduledBackupJob
from prime_backup.mcdr.task_manager import TaskManager


class CrontabManager:
	def __init__(self, task_manager: TaskManager):
		self.task_manager = task_manager
		self.logger = logger.get()
		self.thread = threading.Thread(target=self.__crontab_loop, name='PB@{}-crontab'.format(constants.INSTANCE_ID), daemon=True)
		self.scheduler = BlockingScheduler(
			logger=self.logger,
			executors={
				'default': ThreadPoolExecutor(
					pool_kwargs=dict(thread_name_prefix='PB@{}-crontab-worker'.format(constants.INSTANCE_ID)),
				)
			},
		)
		jobs = [
			ScheduledBackupJob(self.scheduler, self.task_manager),
			PruneBackupJob(self.scheduler, self.task_manager)
		]
		self.jobs: Dict[CrontabJobId, CrontabJob] = {job.id: job for job in jobs}

	def start(self):
		self.thread.start()
		for job in self.jobs.values():
			job.enable()

	def shutdown(self):
		self.send_event(CrontabJobEvent.plugin_unload)
		if self.thread.is_alive():
			self.scheduler.shutdown()
			self.thread.join()

	def get_job(self, job_id: CrontabJobId) -> CrontabJob:
		return self.jobs[job_id]

	def __crontab_loop(self):
		try:
			self.scheduler.start()
		except Exception as e:
			self.logger.error('scheduler loop error: {}'.format(e))
			raise

	def send_event(self, event: CrontabJobEvent):
		for job in self.jobs.values():
			job.on_event(event)
