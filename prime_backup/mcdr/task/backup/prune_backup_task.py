import collections
import contextlib
import datetime
import functools
import logging
import threading
import time
from typing import List, NamedTuple, Dict, Union, Optional, Callable, ContextManager, Tuple

import pytz
from mcdreforged.api.all import *

from prime_backup import constants
from prime_backup.action.delete_backup_action import DeleteBackupAction
from prime_backup.action.list_backup_action import ListBackupAction
from prime_backup.config.prune_config import PruneSetting
from prime_backup.exceptions import BackupNotFound
from prime_backup.mcdr.task import OperationTask, TaskEvent
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.blob_info import BlobListSummary
from prime_backup.types.units import ByteCount
from prime_backup.utils import misc_utils


class PruneMark(NamedTuple):
	keep: bool
	reason: str

	def is_protected(self) -> bool:
		return self.keep and self.reason == 'protected'

	@classmethod
	def create_keep(cls, reason: str) -> 'PruneMark':
		return PruneMark(True, reason)

	@classmethod
	def create_protected(cls) -> 'PruneMark':
		return PruneMark(True, 'protected')

	@classmethod
	def create_remove(cls, reason: str) -> 'PruneMark':
		return PruneMark(False, reason)


class PruneResultItem(NamedTuple):
	backup: BackupInfo
	mark: PruneMark


class PruneResult(List[PruneResultItem]):
	def get_keep_reason(self, backup_or_id: Union[int, BackupInfo]) -> Optional[str]:
		if isinstance(backup_or_id, BackupInfo):
			backup_or_id = backup_or_id.id
		mark = self.id_to_mark[backup_or_id]
		if mark.keep:
			return mark.reason
		return None

	@functools.cached_property
	def id_to_mark(self) -> Dict[int, PruneMark]:
		return {backup.id: mark for backup, mark in self}


class PruneBackupTask(OperationTask):
	def __init__(self, source: CommandSource, backup_filter: BackupFilter, setting: PruneSetting, *, what_to_prune: Optional[RTextBase] = None):
		super().__init__(source)
		self.backup_filter = backup_filter
		self.setting = setting
		if not setting.enabled:
			raise ValueError('the prune setting should be enabled')
		self.what_to_prune = what_to_prune
		self.is_aborted = threading.Event()

	@property
	def name(self) -> str:
		return 'prune'

	def is_abort_able(self) -> bool:
		return True

	@contextlib.contextmanager
	def open_prune_logger(self) -> ContextManager[logging.Logger]:
		logger = logging.Logger(f'{constants.PLUGIN_ID}-prune')
		logger.setLevel(logging.DEBUG if self.config.debug else logging.INFO)
		handler = logging.FileHandler(self.config.storage_path / 'logs' / 'prune.log')
		handler.setFormatter(logging.Formatter('[%(asctime)s %(levelname)s] (%(funcName)s) %(message)s'))
		logger.addHandler(handler)
		try:
			yield logger
		finally:
			logger.removeHandler(handler)

	@classmethod
	def calc_prune_backups(cls, backups: List[BackupInfo], settings: PruneSetting, *, timezone: Optional[datetime.tzinfo] = None) -> PruneResult:
		marks: Dict[int, PruneMark] = {}
		fallback_marks: Dict[int, PruneMark] = {}
		backups = list(sorted(backups, key=lambda b: b.timestamp_ns, reverse=True))  # new -> old

		def has_mark(backup: BackupInfo, keep: bool, protect: Optional[bool] = None) -> bool:
			if (m := marks.get(backup.id)) is None:
				return False
			return m.keep == keep and (protect is None or m.is_protected() == protect)

		# ref: https://github.com/proxmox/proxmox-backup/blob/master/pbs-datastore/src/prune.rs
		def mark_selections(limit: int, policy: str, bucket_mapper: Callable[[BackupInfo], str]):
			already_included: Dict[str, BackupInfo] = {}
			handled_buckets: Dict[str, BackupInfo] = {}
			for backup in backups:
				if has_mark(backup, True, False):
					already_included[bucket_mapper(backup)] = backup

			for backup in backups:
				if backup.id in marks:
					continue
				if backup.tags.is_protected():
					marks[backup.id] = PruneMark.create_protected()
					continue
				bucket = bucket_mapper(backup)
				if bucket in already_included:
					existed = already_included[bucket]
					fallback_marks[backup.id] = fallback_marks.get(backup.id) or PruneMark.create_remove(f'superseded by {existed.id} ({policy})')
					continue
				if bucket in handled_buckets:
					existed = handled_buckets[bucket]
					marks[backup.id] = PruneMark.create_remove(f'superseded by {existed.id} ({policy})')
				else:
					if 0 <= limit <= len(handled_buckets):
						break
					handled_buckets[bucket] = backup
					marks[backup.id] = PruneMark.create_keep(f'keep {policy} {len(handled_buckets)}')

		def create_time_str_func(fmt: str):
			def func(backup: BackupInfo):
				timestamp = backup.timestamp_ns / 1e9
				dt = datetime.datetime.fromtimestamp(timestamp, tz=timezone)
				return dt.strftime(fmt)
			return func

		if settings.last != 0:
			mark_selections(settings.last, 'last', lambda b: str(b.id))
		if settings.hour != 0:
			mark_selections(settings.hour, 'hour', create_time_str_func('%Y/%m/%d/%H'))
		if settings.day != 0:
			mark_selections(settings.day, 'day', create_time_str_func('%Y/%m/%d'))
		if settings.week != 0:
			mark_selections(settings.week, 'week', create_time_str_func('%G/%V'))
		if settings.month != 0:
			mark_selections(settings.month, 'month', create_time_str_func('%Y/%m'))
		if settings.year != 0:
			mark_selections(settings.year, 'year', create_time_str_func('%Y'))

		result = PruneResult()
		now = time.time_ns()
		regular_keep_count = 0
		all_marks = collections.ChainMap(marks, fallback_marks)
		default_mark = PruneMark.create_remove('unmarked')
		for backup_info in backups:
			if backup_info.tags.is_protected():
				result.append(PruneResultItem(backup_info, PruneMark.create_protected()))
			else:
				mark = all_marks.get(backup_info.id, default_mark)
				if mark.keep:
					if 0 < settings.max_amount <= regular_keep_count:
						mark = PruneMark.create_remove('max_amount exceeded')
					elif 0 < settings.max_lifetime.value < (now - backup_info.timestamp_ns):
						mark = PruneMark.create_remove('max_lifetime exceeded')

				result.append(PruneResultItem(backup_info, mark))
				if mark.keep:
					regular_keep_count += 1
		return result

	def __msg_header(self) -> RTextBase:
		return RTextList('(', self.what_to_prune, ') ').set_color(RColor.gray)

	def reply(self, msg: Union[str, RTextBase], *, with_prefix: bool = True):
		if self.what_to_prune is not None:
			msg = self.__msg_header() + msg
		super().reply(msg, with_prefix=with_prefix)

	def run(self) -> Tuple[int, BlobListSummary]:  # backup count, bls sum
		backups = ListBackupAction(calc_size=False, backup_filter=self.backup_filter).run()
		backup_ids = {backup.id for backup in backups}

		timezone: Optional[datetime.tzinfo] = None
		if (timezone_override := self.config.prune.timezone_override) is not None:
			try:
				timezone = pytz.timezone(timezone_override)
			except pytz.UnknownTimeZoneError as e:
				self.logger.error('Bad timezone override from config, using local timezone: {}'.format(e))
			else:
				timezone = None

		result = self.calc_prune_backups(backups, self.setting, timezone=timezone)
		for pri in result:
			misc_utils.assert_true(pri.backup.id in backup_ids, lambda: 'unexpected backup id {}, {}'.format(pri.backup.id, backup_ids))

		bls = BlobListSummary.zero()
		cnt = 0
		with self.open_prune_logger() as prune_logger:
			prune_logger.info('Prune started')
			to_deleted_ids = [pri.backup.id for pri in result if not pri.mark.keep]
			if len(to_deleted_ids) == 0:
				self.reply(self.tr('nothing_to_prune'))
				prune_logger.info('Nothing to prune')
				return cnt, bls

			prune_logger.info('============== Prune calculate result start ==============')
			for pri in result:
				prune_logger.info('Backup #{} at {}: keep={} reason={}'.format(pri.backup.id, pri.backup.date_str, pri.mark.keep, pri.mark.reason))
			prune_logger.info('============== Prune calculate result end ==============')

			self.reply(self.tr(
				'list_to_be_pruned',
				TextComponents.number(len(to_deleted_ids)),
				RTextList(
					'[',
					RTextBase.join(', ', map(functools.partial(TextComponents.backup_id, hover=False, click=False), to_deleted_ids)),
					']',
				),
			))

			for pri in result:
				bid = pri.backup.id
				if self.is_aborted.is_set():
					self.reply(self.tr('aborted'))
					break
				if not pri.mark.keep:
					self.reply(self.tr('prune', TextComponents.backup_id(bid, hover=False, click=False)))
					try:
						dr = DeleteBackupAction(bid).run()
					except Exception as e:
						if isinstance(e, BackupNotFound):
							prune_logger.error('Delete backup #%s resulting in BackupNotFound', bid)
						else:
							prune_logger.exception('Delete backup #%s error', bid)
							raise
					else:
						prune_logger.info('Delete backup #%s done', bid)
						bls = bls + dr.bls
						cnt += 1
			for logger in [self.logger, prune_logger]:
				logger.info('Pruned backup done, deleted {} backups, freed {}'.format(
					bls.count, ByteCount(bls.stored_size).auto_str(), ByteCount(bls.raw_size).auto_str()
				))

		self.reply(self.tr('done', cnt, TextComponents.number(bls.count), TextComponents.blob_list_summary_store_size(bls)))
		return cnt, bls

	def on_event(self, event: TaskEvent):
		if event in [TaskEvent.plugin_unload, TaskEvent.operation_aborted]:
			self.is_aborted.set()


class PruneAllBackupTask(OperationTask):
	def __init__(self, source: CommandSource):
		super().__init__(source)
		self.__current_task: Optional[PruneBackupTask] = None
		self.is_aborted = threading.Event()

	@property
	def name(self) -> str:
		return 'prune_all'

	def is_abort_able(self) -> bool:
		return True

	def run(self) -> Tuple[int, BlobListSummary]:
		config = self.config.prune
		cnt_sum, bls_sum = 0, BlobListSummary.zero()
		if not config.regular_backup.enabled and not config.pre_restore_backup.enabled:
			self.reply(self.tr('nothing_to_do'))
			return cnt_sum, bls_sum

		def prune_backups(what: str, backup_filter: BackupFilter, setting: PruneSetting) -> Tuple[int, BlobListSummary]:
			if not setting.enabled or self.is_aborted.is_set():
				return BlobListSummary.zero()

			self.reply(self.tr('start', self.tr(f'what.{what}')))

			self.__current_task = PruneBackupTask(self.source, backup_filter, setting, what_to_prune=self.tr(f'what.{what}'))
			cnt, bls = self.__current_task.run()
			self.__current_task = None

			nonlocal cnt_sum, bls_sum
			cnt_sum += cnt
			bls_sum = bls_sum + bls

		prune_backups('regular', BackupFilter().filter_non_pre_restore_backup(), config.regular_backup)
		prune_backups('pre_restore', BackupFilter().filter_pre_restore_backup(), config.pre_restore_backup)

		self.reply(self.tr('done', cnt_sum, TextComponents.number(bls_sum.count), TextComponents.blob_list_summary_store_size(bls_sum)))
		return cnt_sum, bls_sum

	def on_event(self, event: TaskEvent):
		if (task := self.__current_task) is not None:
			task.on_event(event)

		if event in [TaskEvent.plugin_unload, TaskEvent.operation_aborted]:
			self.is_aborted.set()


def __main():
	id_counter = 0
	backups = []

	def add(dt: datetime.datetime):
		nonlocal id_counter
		id_counter += 1

		from prime_backup.types.backup_tags import BackupTags
		from prime_backup.types.operator import Operator
		backups.append(BackupInfo(
			id=id_counter, timestamp_ns=int(dt.timestamp() * 1e9),
			author=Operator.pb('test'), comment='', tags=BackupTags(), raw_size=0, stored_size=0,
		))

	date = datetime.datetime.now().replace(hour=0, minute=10, second=0, microsecond=0) - datetime.timedelta(hours=1)
	for i in range(150):
		add(date)
		if i < 30:
			date -= datetime.timedelta(minutes=10)
		else:
			date -= datetime.timedelta(hours=6)

	settings = PruneSetting(last=7, hour=2, day=4, week=3, max_amount=15)
	result = PruneBackupTask.calc_prune_backups(backups, settings)

	for pri in result:
		print(pri.backup.id, pri.backup.date, pri.mark.keep, pri.mark.reason)


if __name__ == '__main__':
	__main()
