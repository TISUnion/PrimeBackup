import dataclasses
import functools
import logging
import os
import stat
import time
from pathlib import Path
from typing import List, Optional, Any, Dict, Generator, Set, ContextManager

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.action.helpers.backup_finalizer import BackupFinalizer
from prime_backup.action.helpers.blob_allocator import BlobAllocator, GetOrCreateBlobResult
from prime_backup.action.helpers.blob_pre_calc_result import BlobPrecalculateResult
from prime_backup.action.helpers.blob_recorder import BlobRecorder
from prime_backup.action.helpers.create_backup_utils import CreateBackupTimeCostKey, SourceFileNotFoundWrapper
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.db.values import FileRole
from prime_backup.exceptions import UnsupportedFileFormat
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.backup_tags import BackupTags
from prime_backup.types.blob_info import BlobListSummary
from prime_backup.types.operator import Operator
from prime_backup.types.units import ByteCount
from prime_backup.utils import sqlalchemy_utils
from prime_backup.utils.thread_pool import FailFastBlockingThreadPool
from prime_backup.utils.time_cost_stats import TimeCostStats


@dataclasses.dataclass(frozen=True)
class _ScanResultEntry:
	path: Path  # full path, including source_root
	stat: os.stat_result

	def is_file(self) -> bool:
		return stat.S_ISREG(self.stat.st_mode)

	def is_dir(self) -> bool:
		return stat.S_ISDIR(self.stat.st_mode)

	def is_symlink(self) -> bool:
		return stat.S_ISLNK(self.stat.st_mode)


@dataclasses.dataclass(frozen=True)
class _ScanResult:
	all_files: List[_ScanResultEntry] = dataclasses.field(default_factory=list)
	root_targets: List[str] = dataclasses.field(default_factory=list)  # list of posix path, related to the source_path


@dataclasses.dataclass(frozen=True)
class _PreCalculationResult:
	stats: Dict[Path, os.stat_result] = dataclasses.field(default_factory=dict)
	hashes_and_chunks: Dict[Path, BlobPrecalculateResult] = dataclasses.field(default_factory=dict)
	reused_files: Dict[Path, schema.File] = dataclasses.field(default_factory=dict)


class CreateBackupAction(Action[BackupInfo]):
	def __init__(self, creator: Operator, comment: str, *, tags: Optional[BackupTags] = None, source_path: Optional[Path] = None):
		super().__init__()
		if tags is None:
			tags = BackupTags()

		self.creator = creator
		self.comment = comment
		self.tags = tags

		self.__source_path: Path = source_path or self.config.source_path
		self.__time_costs: TimeCostStats[CreateBackupTimeCostKey] = TimeCostStats()
		self.__pre_calc_result = _PreCalculationResult()
		self.__new_blobs_summary = BlobListSummary.zero()

	def __file_path_to_db_path(self, path: Path) -> str:
		return path.relative_to(self.__source_path).as_posix()

	def __scan_files(self) -> _ScanResult:
		ignore_or_retained_patterns = self.config.backup.ignore_or_retained_patterns_spec
		result = _ScanResult()
		visited_path: Set[Path] = set()  # full path
		ignored_or_retained_paths: List[Path] = []   # related path

		def scan(full_path: Path, is_root_target: bool):
			try:
				rel_path = full_path.relative_to(self.__source_path)
			except ValueError:
				self.logger.warning("Skipping backup path {!r} cuz it's not inside the source path {!r}".format(str(full_path), str(self.__source_path)))
				return

			if ignore_or_retained_patterns.match_file(rel_path) or self.config.backup.is_file_ignore_by_deprecated_ignored_files(rel_path.name):
				ignored_or_retained_paths.append(rel_path)
				if is_root_target:
					self.logger.warning('Backup target {!r} is ignored or retained by config'.format(str(rel_path)))
				return

			if full_path in visited_path:
				return
			visited_path.add(full_path)

			try:
				st = full_path.lstat()
			except FileNotFoundError:
				if is_root_target:
					self.logger.warning('Backup target {!r} does not exist, skipped. full_path: {!r}'.format(str(rel_path), str(full_path)))
				return

			entry = _ScanResultEntry(full_path, st)
			result.all_files.append(entry)
			if is_root_target:
				result.root_targets.append(rel_path.as_posix())

			if entry.is_dir():
				for child in os.listdir(full_path):
					scan(full_path / child, False)
			elif is_root_target and entry.is_symlink() and self.config.backup.follow_target_symlink:
				symlink_target = full_path.readlink()
				symlink_target_full_path = self.__source_path / symlink_target
				self.logger.info('Following root symlink target {!r} -> {!r} ({!r})'.format(str(rel_path), str(symlink_target), str(symlink_target_full_path)))
				scan(symlink_target_full_path, True)

		self.logger.debug(f'Scan file start, target patterns: {self.config.backup.targets}')
		with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_fs) as scan_cost:
			target_patterns = self.config.backup.targets_spec
			target_paths: List[Path] = []
			for candidate_target_name in sorted(os.listdir(self.__source_path)):
				candidate_target_path = self.__source_path / candidate_target_name
				if target_patterns.match_file(candidate_target_name):
					target_paths.append(candidate_target_path)

			self.logger.debug(f'Scan file found {len(target_paths)} targets, {target_paths[:10]=}')
			for target_path in target_paths:
				scan(target_path, True)

		self.logger.debug('Scan file done, cost {:.2f}s, count {}, root_targets (len={}): {}, ignored_or_retained_paths[:100] (len={}): {}'.format(
			scan_cost(), len(result.all_files),
			len(result.root_targets), result.root_targets,
			len(ignored_or_retained_paths), [p.as_posix() for p in ignored_or_retained_paths][:100],
		))
		return result

	def __pre_calculate_stats(self, scan_result: _ScanResult):
		stats = self.__pre_calc_result.stats
		stats.clear()
		for file_entry in scan_result.all_files:
			stats[file_entry.path] = file_entry.stat

	def __reuse_unchanged_files(self, session: DbSession, scan_result: _ScanResult):
		with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_db):
			backup = session.get_last_backup()
		if backup is None:
			return

		@dataclasses.dataclass(frozen=True)
		class StatKey:
			path: str
			size: Optional[int]  # it shouldn't be None, but just in case
			mode: int
			uid: int
			gid: int
			mtime_us: int

		with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_db):
			backup_files = session.get_backup_files(backup.id)

		stat_to_files: Dict[StatKey, schema.File] = {}
		for file in backup_files:
			if stat.S_ISREG(file.mode):
				if file.uid is None or file.gid is None or file.mtime is None:
					raise AssertionError('file {!r} with ISREG mode has missing fields')
				key = StatKey(
					path=file.path,
					size=file.blob_raw_size,
					mode=file.mode,
					uid=file.uid,
					gid=file.gid,
					mtime_us=file.mtime,
				)
				stat_to_files[key] = file

		for file_entry in scan_result.all_files:
			if file_entry.is_file():
				key = StatKey(
					path=self.__file_path_to_db_path(file_entry.path),
					size=file_entry.stat.st_size,
					mode=file_entry.stat.st_mode,
					uid=file_entry.stat.st_uid,
					gid=file_entry.stat.st_gid,
					mtime_us=file_entry.stat.st_mtime_ns // 1000
				)
				if (file_opt := stat_to_files.get(key)) is not None:
					self.__pre_calc_result.reused_files[file_entry.path] = file_opt

	def __pre_calculate_hash_and_chunks(self, session: DbSession, blob_allocator: BlobAllocator, scan_result: _ScanResult):
		hashes_and_chunks = self.__pre_calc_result.hashes_and_chunks
		hashes_and_chunks.clear()

		file_entries_to_hash: List[_ScanResultEntry] = [
			file_entry
			for file_entry in scan_result.all_files
			if file_entry.is_file() and file_entry.path not in self.__pre_calc_result.reused_files
		]

		all_sizes: Set[int] = {file_entry.stat.st_size for file_entry in file_entries_to_hash}
		existing_sizes = session.has_blob_with_size_batched(list(all_sizes))
		blob_allocator.add_existing_sizes(existing_sizes)

		def hash_worker(pth: Path, pth_size: int):
			rel_path = pth.relative_to(self.__source_path)
			try:
				result = BlobPrecalculateResult.from_file(pth, rel_path, pth_size)
			except BlobPrecalculateResult.SizeMismatched:
				return  # the file keeps changing, so it's not good to create a pre-calc result for it
			hashes_and_chunks[pth] = result

		with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_io_read):
			with FailFastBlockingThreadPool(name='hasher') as pool:
				for file_entry in file_entries_to_hash:
					if existing_sizes[file_entry.stat.st_size]:
						# we need to hash the file, sooner or later
						pool.submit(hash_worker, file_entry.path, file_entry.stat.st_size)
					else:
						pass  # will use hash_once policy

	@functools.cached_property
	def __temp_path(self) -> Path:
		p = self.config.temp_path
		p.mkdir(parents=True, exist_ok=True)
		return p

	def __create_file(self, session: DbSession, blob_allocator: BlobAllocator, path: Path) -> Generator[Any, Any, schema.File]:
		if (reused_file := self.__pre_calc_result.reused_files.get(path)) is not None:
			# make a copy
			return session.create_file(
				path=sqlalchemy_utils.mapped_cast(reused_file.path),
				role=FileRole.unknown.value,
				mode=sqlalchemy_utils.mapped_cast(reused_file.mode),
				content=sqlalchemy_utils.mapped_cast(reused_file.content),
				blob_id=sqlalchemy_utils.mapped_cast(reused_file.blob_id),
				blob_storage_method=sqlalchemy_utils.mapped_cast(reused_file.blob_storage_method),
				blob_hash=sqlalchemy_utils.mapped_cast(reused_file.blob_hash),
				blob_compress=sqlalchemy_utils.mapped_cast(reused_file.blob_compress),
				blob_raw_size=sqlalchemy_utils.mapped_cast(reused_file.blob_raw_size),
				blob_stored_size=sqlalchemy_utils.mapped_cast(reused_file.blob_stored_size),
				uid=sqlalchemy_utils.mapped_cast(reused_file.uid),
				gid=sqlalchemy_utils.mapped_cast(reused_file.gid),
				mtime=sqlalchemy_utils.mapped_cast(reused_file.mtime),
			)

		if (st := self.__pre_calc_result.stats.pop(path, None)) is None:
			with SourceFileNotFoundWrapper.wrap(path), self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_fs):
				st = path.lstat()

		blob: Optional[schema.Blob] = None
		content: Optional[bytes] = None
		if stat.S_ISREG(st.st_mode):
			gen = blob_allocator.get_or_create_blob(path, st)
			try:
				query = gen.send(None)
				while True:
					result = yield query
					query = gen.send(result)
			except StopIteration as e:
				goc_result: GetOrCreateBlobResult = e.value
				blob = goc_result.blob
				st = goc_result.st
				# notes: st.st_size might be incorrect, use blob.raw_size instead if needed
		elif stat.S_ISDIR(st.st_mode):
			pass
		elif stat.S_ISLNK(st.st_mode):
			with SourceFileNotFoundWrapper.wrap(path):
				content = os.readlink(path).encode('utf8')
		else:
			raise UnsupportedFileFormat(st.st_mode)

		return session.create_file(
			path=self.__file_path_to_db_path(path),
			role=FileRole.unknown.value,

			mode=st.st_mode,
			content=content,
			uid=st.st_uid,
			gid=st.st_gid,
			mtime=st.st_mtime_ns // 1000,

			blob=blob,
		)

	def __create_backup(self, session_context: ContextManager[DbSession], session: DbSession, blob_recorder: BlobRecorder) -> BackupInfo:
		def pre_calc_result_getter(src_path: Path) -> Optional[BlobPrecalculateResult]:
			return self.__pre_calc_result.hashes_and_chunks.pop(src_path, None)  # one-time use

		blob_allocator = BlobAllocator(
			session=session,
			time_costs=self.__time_costs,
			blob_recorder=blob_recorder,
			source_path=self.__source_path,
			temp_path=self.__temp_path,
			pre_calc_result_getter=pre_calc_result_getter,
		)

		self.logger.info('Scanning file for backup creation at path {!r}, targets: {}'.format(
			self.__source_path.as_posix(), self.config.backup.targets,
		))
		with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.stage_scan_files):
			scan_result = self.__scan_files()
		backup = session.create_backup(
			creator=str(self.creator),
			comment=self.comment,
			targets=scan_result.root_targets,
			tags=self.tags.to_dict(),
		)
		self.logger.info('Creating backup for {} at path {!r}, file cnt {}, timestamp {!r}, creator {!r}, comment {!r}, tags {!r}'.format(
			scan_result.root_targets, self.__source_path.as_posix(), len(scan_result.all_files),
			backup.timestamp, backup.creator, backup.comment, backup.tags,
		))

		self.__pre_calculate_stats(scan_result)
		if self.config.backup.reuse_stat_unchanged_file:
			with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.stage_reuse_unchanged_files):
				self.__reuse_unchanged_files(session, scan_result)
			self.logger.info('Reused {} / {} stat unchanged files'.format(len(self.__pre_calc_result.reused_files), len(scan_result.all_files)))
		if self.config.get_effective_concurrency() > 1:
			with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.stage_pre_calculate_hash):
				self.__pre_calculate_hash_and_chunks(session, blob_allocator, scan_result)
			self.logger.info('Pre-calculate all file hash done')

		blob_allocator.init_blob_store()

		with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.stage_create_files):
			files = blob_allocator.schedule_loop([
				self.__create_file(session, blob_allocator, file_entry.path)
				for file_entry in scan_result.all_files
			])

		with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.stage_finalize):
			BackupFinalizer(session).finalize_files_and_backup(backup, files)
		info = BackupInfo.of(backup)

		with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.stage_flush_db, CreateBackupTimeCostKey.kind_db):
			session_context.__exit__(None, None, None)
		return info

	@override
	def run(self) -> BackupInfo:
		# TODO: prevent re-run
		self.__time_costs.reset()
		with self.__time_costs.measure_time_cost(*CreateBackupTimeCostKey):
			pass
		action_start_ts = time.time()

		blob_recorder = BlobRecorder()
		try:
			session_context = DbAccess.open_session()
			with session_context as session:
				info = self.__create_backup(session_context, session, blob_recorder)
		except Exception as e:
			blob_recorder.apply_file_rollback()
			raise e

		s = blob_recorder.get_new_blobs_summary()
		self.logger.info('Create backup #{} done, +{} blobs (size {} / {})'.format(
			info.id, s.count, ByteCount(s.stored_size).auto_str(), ByteCount(s.raw_size).auto_str(),
		))
		self.__log_costs(time.time() - action_start_ts)

		self.__new_blobs_summary = blob_recorder.get_new_blobs_summary()
		return info

	def get_new_blobs_summary(self) -> BlobListSummary:
		return self.__new_blobs_summary

	def __log_costs(self, actual_cost: float):
		if not (self.config.debug and self.logger.isEnabledFor(logging.DEBUG)):
			return

		def log_one_key(what: str, cost: float):
			self.logger.debug('  {}: {:.3f}s ({:.1f}%)'.format(what, cost, 100.0 * cost / actual_cost))

		self.logger.debug('========================')
		self.logger.debug('{} run costs'.format(self.__class__.__name__))
		log_one_key('ACTUAL', actual_cost)

		all_costs = self.__time_costs.get_costs()
		kind_costs = {k: v for k, v in all_costs.items() if k.name.startswith('kind_')}
		stage_costs = {k: v for k, v in all_costs.items() if k.name.startswith('stage_')}

		self.logger.debug('Kind costs')
		for k, v in kind_costs.items():
			log_one_key(k.name, v)
		log_one_key('rest', actual_cost - sum(kind_costs.values()))
		self.logger.debug('Stage costs')
		for k, v in stage_costs.items():
			log_one_key(k.name, v)
		log_one_key('rest', actual_cost - sum(stage_costs.values()))
		self.logger.debug('========================')
