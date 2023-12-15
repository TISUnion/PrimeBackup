import threading
from typing import Dict, Iterable

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.blocking import BlockingScheduler

from prime_backup import logger
from prime_backup.mcdr.crontab_job import CrontabJob, CrontabJobId, CrontabJobEvent
from prime_backup.mcdr.crontab_job.create_db_backup_job import CreateDbBackupJob
from prime_backup.mcdr.crontab_job.prune_backup_job import PruneBackupJob
from prime_backup.mcdr.crontab_job.scheduled_backup_job import ScheduledBackupJob
from prime_backup.mcdr.crontab_job.vacuum_sqlite_job import VacuumSqliteJob
from prime_backup.mcdr.task_manager import TaskManager
from prime_backup.utils import misc_utils


class CrontabManager:
	def __init__(self, task_manager: TaskManager):
		self.task_manager = task_manager
		self.logger = logger.get()
		self.thread = threading.Thread(target=self.__crontab_loop, name=misc_utils.make_thread_name('crontab-scheduler'), daemon=True)
		self.scheduler = BlockingScheduler(
			logger=self.logger,
			executors={
				'default': ThreadPoolExecutor(
					pool_kwargs=dict(thread_name_prefix=misc_utils.make_thread_name('crontab')),
				)
			},
		)
		job_classes = [
			CreateDbBackupJob,
			PruneBackupJob,
			ScheduledBackupJob,
			VacuumSqliteJob,
		]
		jobs = [clazz(self.scheduler, self.task_manager) for clazz in job_classes]
		self.jobs: Dict[CrontabJobId, CrontabJob] = {job.id: job for job in jobs}

		self.__no_more_event = False

	def start(self):
		self.thread.start()
		for job in self.jobs.values():
			job.enable()

	def shutdown(self):
		self.send_event(CrontabJobEvent.plugin_unload)
		self.__no_more_event = True
		if self.thread.is_alive():
			self.scheduler.shutdown()
			self.thread.join()

	def get_job(self, job_id: CrontabJobId) -> CrontabJob:
		return self.jobs[job_id]

	def list_jobs(self) -> Iterable[CrontabJob]:
		return self.jobs.values()

	def __crontab_loop(self):
		try:
			self.scheduler.start()
		except Exception as e:
			self.logger.error('scheduler loop error: {}'.format(e))
			raise

	def send_event(self, event: CrontabJobEvent):
		if self.__no_more_event:
			return

		for job in self.jobs.values():
			job.on_event(event)
