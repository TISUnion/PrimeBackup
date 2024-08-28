import collections
import dataclasses
import datetime
import functools
import time
from typing import List, Dict, Union, Optional, Callable

import pytz
from mcdreforged.api.all import *

from prime_backup.action.delete_backup_action import DeleteBackupAction
from prime_backup.action.list_backup_action import ListBackupAction
from prime_backup.config.prune_config import PruneSetting
from prime_backup.exceptions import BackupNotFound
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.blob_info import BlobListSummary
from prime_backup.types.operator import PrimeBackupOperatorNames
from prime_backup.types.units import ByteCount
from prime_backup.utils import misc_utils, log_utils


class _PruneVerbose:
	silent = 0
	delete = 1
	all = 2


@dataclasses.dataclass(frozen=True)
class PruneMark:
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


@dataclasses.dataclass(frozen=True)
class PrunePlanItem:
	backup: BackupInfo
	mark: PruneMark


class PrunePlan(List[PrunePlanItem]):
	def get_keep_reason(self, backup_or_id: Union[int, BackupInfo]) -> Optional[str]:
		if isinstance(backup_or_id, BackupInfo):
			backup_or_id = backup_or_id.id
		mark = self.id_to_mark[backup_or_id]
		if mark.keep:
			return mark.reason
		return None

	@functools.cached_property
	def id_to_mark(self) -> Dict[int, PruneMark]:
		return {pri.backup.id: pri.mark for pri in self}


@dataclasses.dataclass
class PruneBackupResult:
	plan: PrunePlan
	deleted_backup_count: int = 0
	deleted_blobs: BlobListSummary = BlobListSummary.zero()


@dataclasses.dataclass
class PruneAllBackupResult:
	sub_plans: List[PrunePlan] = dataclasses.field(default_factory=list)
	deleted_backup_count: int = 0
	deleted_blobs: BlobListSummary = BlobListSummary.zero()


class PruneBackupTask(HeavyTask[PruneBackupResult]):
	def __init__(self, source: CommandSource, backup_filter: BackupFilter, setting: PruneSetting, *, what_to_prune: Optional[RTextBase] = None, verbose: int = 2):
		super().__init__(source)
		self.backup_filter = backup_filter
		self.setting = setting
		if not setting.enabled:
			raise ValueError('the prune setting should be enabled')
		self.what_to_prune = what_to_prune
		self.verbose = verbose

	@property
	def id(self) -> str:
		return 'backup_prune'

	def is_abort_able(self) -> bool:
		return True

	@classmethod
	def calc_prune_backups(cls, backups: List[BackupInfo], settings: PruneSetting, *, timezone: Optional[datetime.tzinfo] = None) -> PrunePlan:
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
			def func(backup: BackupInfo) -> str:
				timestamp = backup.timestamp_ns / 1e9
				dt = datetime.datetime.fromtimestamp(timestamp, tz=timezone)
				return dt.strftime(fmt)
			return func

		if settings.last != 0:
			def __backup_to_id(b: BackupInfo) -> str:
				return str(b.id)
			mark_selections(settings.last, 'last', __backup_to_id)
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

		plan_list = PrunePlan()
		now = time.time_ns()
		regular_keep_count = 0
		all_marks = collections.ChainMap(marks, fallback_marks)
		default_mark = PruneMark.create_remove('unmarked')
		for backup_info in backups:
			if backup_info.tags.is_protected():
				plan_list.append(PrunePlanItem(backup_info, PruneMark.create_protected()))
			else:
				mark = all_marks.get(backup_info.id, default_mark)
				if mark.keep:
					if 0 < settings.max_amount <= regular_keep_count:
						mark = PruneMark.create_remove('max_amount exceeded')
					elif 0 < settings.max_lifetime.value_nano < (now - backup_info.timestamp_ns):
						mark = PruneMark.create_remove('max_lifetime exceeded')

				plan_list.append(PrunePlanItem(backup_info, mark))
				if mark.keep:
					regular_keep_count += 1
		return plan_list

	def __msg_header(self) -> RTextBase:
		return RTextList('(', self.what_to_prune, ') ').set_color(RColor.gray)

	def reply(self, msg: Union[str, RTextBase], *, with_prefix: bool = True):
		if self.what_to_prune is not None:
			msg = self.__msg_header() + msg
		super().reply(msg, with_prefix=with_prefix)

	def run(self) -> PruneBackupResult:
		backups = ListBackupAction(backup_filter=self.backup_filter).run()
		backup_ids = {backup.id for backup in backups}

		timezone: Optional[datetime.tzinfo] = None
		if (timezone_override := self.config.prune.timezone_override) is not None:
			try:
				timezone = pytz.timezone(timezone_override)
			except pytz.UnknownTimeZoneError as e:
				self.logger.error('Bad timezone override from config, using local timezone: {}'.format(e))
			else:
				timezone = None

		plan_list = self.calc_prune_backups(backups, self.setting, timezone=timezone)
		for pl in plan_list:
			misc_utils.assert_true(pl.backup.id in backup_ids, lambda: 'unexpected backup id {}, {}'.format(pl.backup.id, backup_ids))

		result = PruneBackupResult(plan_list)
		with log_utils.open_file_logger('prune') as prune_logger:
			prune_logger.info('Prune started')
			to_deleted_ids = [pl.backup.id for pl in plan_list if not pl.mark.keep]
			if len(to_deleted_ids) == 0:
				if self.verbose >= _PruneVerbose.all:
					self.reply_tr('nothing_to_prune')
				prune_logger.info('Nothing to prune')
				return result

			prune_logger.info('============== Prune calculate result start ==============')
			for pl in plan_list:
				prune_logger.info('Backup #{} at {}: keep={} reason={}'.format(pl.backup.id, pl.backup.date_str, pl.mark.keep, pl.mark.reason))
			prune_logger.info('============== Prune calculate result end ==============')

			if self.verbose >= _PruneVerbose.delete:
				self.reply_tr(
					'list_to_be_pruned',
					TextComponents.number(len(to_deleted_ids)),
					TextComponents.backup_id_list(to_deleted_ids, hover=False, click=False),
				)

			for pl in plan_list:
				bid = pl.backup.id
				if self.aborted_event.is_set():
					if self.verbose >= _PruneVerbose.delete:
						self.reply(self.get_aborted_text())
					break
				if not pl.mark.keep:
					self.reply_tr('prune', TextComponents.backup_id(bid, hover=False, click=False))
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
						result.deleted_blobs = result.deleted_blobs + dr.bls
						result.deleted_backup_count += 1
			for logger in [self.logger, prune_logger]:
				logger.info('Pruned backup done, deleted {} backups, freed {} blobs ({} / {})'.format(
					result.deleted_backup_count, result.deleted_blobs.count,
					ByteCount(result.deleted_blobs.stored_size).auto_str(), ByteCount(result.deleted_blobs.raw_size).auto_str(),
				))

		if self.verbose >= _PruneVerbose.delete:
			self.reply_tr(
				'done',
				TextComponents.number(result.deleted_backup_count),
				TextComponents.number(result.deleted_blobs.count),
				TextComponents.blob_list_summary_store_size(result.deleted_blobs),
			)
		return result


class PruneAllBackupTask(HeavyTask[PruneAllBackupResult]):
	def __init__(self, source: CommandSource, verbose: int = 2):
		super().__init__(source)
		self.verbose = verbose

	@property
	def id(self) -> str:
		return 'backup_prune_all'

	def is_abort_able(self) -> bool:
		return True

	def run(self) -> PruneAllBackupResult:
		config = self.config.prune
		result = PruneAllBackupResult()
		if not config.regular_backup.enabled and not config.temporary_backup.enabled:
			if self.verbose >= _PruneVerbose.all:
				self.reply_tr('nothing_to_do')
			return result

		def prune_backups(what: str, backup_filter: BackupFilter, setting: PruneSetting):
			if not setting.enabled or self.aborted_event.is_set():
				return

			if self.verbose >= _PruneVerbose.all:
				self.reply_tr('start', self.tr(f'what.{what}'))

			sub_result = self.run_subtask(PruneBackupTask(self.source, backup_filter, setting, what_to_prune=self.tr(f'what.{what}'), verbose=self.verbose))
			result.sub_plans.append(sub_result.plan)
			result.deleted_backup_count += sub_result.deleted_backup_count
			result.deleted_blobs = result.deleted_blobs + sub_result.deleted_blobs

		prune_backups('regular', BackupFilter().filter_non_temporary_backup(), config.regular_backup)
		if not self.aborted_event.is_set():
			prune_backups('temporary', BackupFilter().filter_temporary_backup(), config.temporary_backup)

		if self.verbose >= _PruneVerbose.delete:
			self.reply_tr(
				'done',
				TextComponents.number(result.deleted_backup_count),
				TextComponents.number(result.deleted_blobs.count),
				TextComponents.blob_list_summary_store_size(result.deleted_blobs),
			)
		return result


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
			creator=Operator.pb(PrimeBackupOperatorNames.test), comment='', targets=[], tags=BackupTags(), raw_size=0, stored_size=0,
			files=[],
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
