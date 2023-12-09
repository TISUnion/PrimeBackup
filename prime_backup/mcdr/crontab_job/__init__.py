import enum
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from apscheduler.schedulers.base import BaseScheduler
from mcdreforged.api.all import *

if TYPE_CHECKING:
	from prime_backup.mcdr.task_manager import TaskManager


class CrontabJobId(enum.Enum):
	create_db_backup = enum.auto()
	prune_backup = enum.auto()
	schedule_backup = enum.auto()
	vacuum_sqlite = enum.auto()


class CrontabJobEvent(enum.Enum):
	plugin_unload = enum.auto()
	manual_backup_created = enum.auto()


class CrontabJob(ABC):
	def __init__(self, scheduler: BaseScheduler, task_manager: 'TaskManager'):
		self.scheduler = scheduler
		self.task_manager = task_manager

	@property
	@abstractmethod
	def id(self) -> CrontabJobId:
		...

	@abstractmethod
	def is_enabled(self) -> bool:
		...

	@abstractmethod
	def enable(self):
		...

	@abstractmethod
	def pause(self):
		...

	@abstractmethod
	def resume(self):
		...

	@abstractmethod
	def is_running(self) -> bool:
		...

	@abstractmethod
	def is_pause(self) -> bool:
		...

	@abstractmethod
	def get_duration_until_next_run_text(self) -> RTextBase:
		...

	@abstractmethod
	def get_next_run_date(self) -> RTextBase:
		...

	@abstractmethod
	def get_name_text(self) -> RTextBase:
		...

	@abstractmethod
	def run(self):
		...

	def on_event(self, event: CrontabJobEvent):
		pass
