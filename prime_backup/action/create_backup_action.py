import dataclasses
import functools
import logging
import os
import stat
import time
from concurrent.futures import Future
from pathlib import Path
from typing import List, Optional, Dict, Set, ContextManager, Iterable
from typing import Tuple

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.action.helpers.backup_finalizer import BackupFinalizer
from prime_backup.action.helpers.blob_allocator import BlobAllocator
from prime_backup.action.helpers.blob_creator_common import BlobCreateFileLookup, BlobLookupRoutine
from prime_backup.action.helpers.blob_pre_calc_result import BlobPrecalculateResult, CalcChunkPolicy
from prime_backup.action.helpers.blob_recorder import BlobRecorder
from prime_backup.action.helpers.create_backup_utils import CreateBackupTimeCostKey, SourceFileNotFoundWrapper
from prime_backup.action.helpers.pack_writer import PackWriter
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.db.values import FileRole, BlobStorageMethod
from prime_backup.exceptions import UnsupportedFileFormat
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.backup_tags import BackupTags
from prime_backup.types.blob_info import BlobDeltaSummary
from prime_backup.types.chunk_method import ChunkMethod
from prime_backup.types.chunker import PrettyChunk
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

	@property
	def all_file_size_sum(self) -> int:
		return sum(entry.stat.st_size for entry in self.all_files)


@dataclasses.dataclass(frozen=True)
class _PreCalculationResult:
	stats: Dict[Path, os.stat_result] = dataclasses.field(default_factory=dict)  # real-world path
	hashes_and_chunks: Dict[Path, BlobPrecalculateResult] = dataclasses.field(default_factory=dict)  # real-world path
	stat_unchanged_files: Dict[Path, schema.File] = dataclasses.field(default_factory=dict)  # real-world path -> unchanged File in old backup
	reused_files: Dict[Path, schema.File] = dataclasses.field(default_factory=dict)  # real-world path
	previous_backup_files: Dict[str, schema.File] = dataclasses.field(default_factory=dict)  # db path, relative to source_path
	previous_file_chunks: Dict[Path, List[PrettyChunk]] = dataclasses.field(default_factory=dict)  # real-world path


class CreateBackupAction(Action[BackupInfo]):
	def __init__(self, creator: Operator, comment: str, *, tags: Optional[BackupTags] = None, source_path: Optional[Path] = None):
		super().__init__()
		if tags is None:
			tags = BackupTags()

		self.creator = creator
		self.comment = comment
		self.tags = tags

		self.__run_called = False
		self.__source_path: Path = source_path or self.config.source_path
		self.__time_costs: TimeCostStats[CreateBackupTimeCostKey] = TimeCostStats()
		self.__pre_calc_result = _PreCalculationResult()
		self.__new_blob_storage_delta = BlobDeltaSummary.zero()

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
				symlink_target_full_path = (full_path.parent / symlink_target).resolve()
				if not symlink_target_full_path.parent.samefile(self.__source_path):
					self.logger.warning('Skipping root symlink target {!r} since it''s target {!r} ({!r}) is outside of the source path'.format(str(rel_path), str(symlink_target), str(symlink_target_full_path)))
					return
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

	def __load_previous_backup_files(self, session: DbSession):
		previous_backup_files = self.__pre_calc_result.previous_backup_files
		previous_backup_files.clear()
		with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_db):
			backup = session.get_last_backup()
		if backup is None:
			return

		with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_db):
			for file in session.get_backup_files(backup):
				previous_backup_files[file.path] = file

	def __collect_stat_unchanged_files(self, scan_result: _ScanResult):
		stat_unchanged_files = self.__pre_calc_result.stat_unchanged_files
		stat_unchanged_files.clear()
		for file_entry in scan_result.all_files:
			if file_entry.is_file():
				db_path = self.__file_path_to_db_path(file_entry.path)
				previous_file = self.__pre_calc_result.previous_backup_files.get(db_path)
				if previous_file is None or not stat.S_ISREG(previous_file.mode):
					continue
				if previous_file.uid is None or previous_file.gid is None or previous_file.mtime is None:
					raise AssertionError('file {!r} with ISREG mode has missing fields'.format(previous_file))
				if (
						previous_file.path == db_path and
						previous_file.blob_raw_size == file_entry.stat.st_size and
						previous_file.mode == file_entry.stat.st_mode and
						previous_file.uid == file_entry.stat.st_uid and
						previous_file.gid == file_entry.stat.st_gid and
						previous_file.mtime_unix_ns == file_entry.stat.st_mtime_ns
				):
					stat_unchanged_files[file_entry.path] = previous_file

	def __reuse_unchanged_files(self):
		reused_files = self.__pre_calc_result.reused_files
		reused_files.clear()
		reused_files.update(self.__pre_calc_result.stat_unchanged_files)

	def __should_collect_stat_unchanged_files(self, scan_result: _ScanResult) -> bool:
		if len(self.__pre_calc_result.previous_backup_files) == 0:
			return False
		if self.config.backup.reuse_stat_unchanged_file:
			return True
		if not self.config.backup.chunking_enabled or len(self.config.backup.chunking_rules) == 0:
			return False

		mutating_patterns_spec = self.config.backup.mutating_file_patterns_spec
		for file_entry in scan_result.all_files:
			if not file_entry.is_file():
				continue
			rel_path = file_entry.path.relative_to(self.__source_path)
			if not mutating_patterns_spec.match_file(rel_path) and ChunkMethod.get_for_file(rel_path, file_entry.stat.st_size) is not None:
				return True
		return False

	def __cache_previous_chunks_for_fixed_auto(self, session: DbSession, scan_result: _ScanResult):
		previous_file_chunks = self.__pre_calc_result.previous_file_chunks
		previous_file_chunks.clear()

		for file_entry in scan_result.all_files:
			if not file_entry.is_file() or file_entry.path in self.__pre_calc_result.reused_files:
				continue

			rel_path = file_entry.path.relative_to(self.__source_path)
			if ChunkMethod.get_for_file(rel_path, file_entry.stat.st_size) != ChunkMethod.fixed_auto:
				continue

			previous_file = self.__pre_calc_result.previous_backup_files.get(rel_path.as_posix())
			if (
					previous_file is None or
					previous_file.blob_id is None or
					previous_file.blob_storage_method != BlobStorageMethod.chunked.value
			):
				continue

			with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_db):
				previous_file_chunks[file_entry.path] = [
					PrettyChunk(offset=offset_chunk.offset, length=offset_chunk.chunk.raw_size, hash=offset_chunk.chunk.hash)
					for offset_chunk in session.get_blob_chunks(previous_file.blob_id)
				]

	@classmethod
	def _pre_calculate_hash_worker(
			cls,
			path: Path,
			rel_path: Path,
			path_size: int,
			previous_chunks: Optional[Iterable[PrettyChunk]],
			calc_chunk_policy: CalcChunkPolicy,
	) -> Optional[BlobPrecalculateResult]:
		try:
			return BlobPrecalculateResult.from_file(
				path, rel_path, path_size,
				previous_chunks=previous_chunks,
				calc_chunk_policy=calc_chunk_policy,
			)
		except BlobPrecalculateResult.SizeMismatched:
			return None  # the file keeps changing, so it's not good to create a pre-calc result for it

	def __pre_calculate_hash_and_chunks(self, session: DbSession, blob_allocator: BlobAllocator, scan_result: _ScanResult):
		hashes_and_chunks = self.__pre_calc_result.hashes_and_chunks
		hashes_and_chunks.clear()

		mutating_patterns_spec = self.config.backup.mutating_file_patterns_spec
		file_entries_to_hash: List[_ScanResultEntry] = [
			file_entry
			for file_entry in scan_result.all_files
			if file_entry.is_file()
			and file_entry.path not in self.__pre_calc_result.reused_files
			and not mutating_patterns_spec.match_file(file_entry.path.relative_to(self.__source_path))
		]

		all_sizes: Set[int] = {file_entry.stat.st_size for file_entry in file_entries_to_hash}
		existing_sizes = session.has_blob_with_size_batched(list(all_sizes))
		blob_allocator.add_existing_sizes(existing_sizes)

		with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_io_read):
			futures: List[Tuple[Path, 'Future[Optional[BlobPrecalculateResult]]']] = []
			with FailFastBlockingThreadPool(name='hasher') as pool:
				for file_entry in file_entries_to_hash:
					if existing_sizes[file_entry.stat.st_size]:
						# we need to hash the file, sooner or later
						path = file_entry.path
						fut: 'Future[Optional[BlobPrecalculateResult]]' = pool.submit(
							self._pre_calculate_hash_worker,
							path=path,
							rel_path=path.relative_to(self.__source_path),
							path_size=file_entry.stat.st_size,
							previous_chunks=self.__pre_calc_result.previous_file_chunks.get(path),
							calc_chunk_policy=CalcChunkPolicy.FALSE if path in self.__pre_calc_result.stat_unchanged_files else CalcChunkPolicy.AUTO,
						)
						futures.append((path, fut))
					else:
						pass  # will use hash_once policy
				for path, fut in futures:
					result = fut.result()
					if result is not None:
						hashes_and_chunks[path] = result

	@functools.cached_property
	def __temp_path(self) -> Path:
		p = self.config.temp_path
		p.mkdir(parents=True, exist_ok=True)
		return p

	def __create_file(self, session: DbSession, blob_allocator: BlobAllocator, path: Path) -> BlobLookupRoutine[schema.File]:
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
				mtime_ns_part=sqlalchemy_utils.mapped_cast(reused_file.mtime_ns_part),
			)

		if (st := self.__pre_calc_result.stats.pop(path, None)) is None:
			with SourceFileNotFoundWrapper.wrap(path), self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_fs):
				st = path.lstat()

		blob: Optional[schema.Blob] = None
		content: Optional[bytes] = None
		if stat.S_ISREG(st.st_mode):
			goc_result = yield from blob_allocator.get_or_create_blob(path, st)
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
			mtime=st.st_mtime_ns // (10 ** 9),
			mtime_ns_part=st.st_mtime_ns % (10 ** 9),

			blob=blob,
		)

	def __create_backup(self, session_context: ContextManager[DbSession], session: DbSession, pack_writer: PackWriter, blob_recorder: BlobRecorder) -> BackupInfo:
		pre_calc_result = self.__pre_calc_result
		file_path_to_db_path = self.__file_path_to_db_path

		class FileLookup(BlobCreateFileLookup):
			@override
			def pop_pre_calc_result(self, src_path: Path) -> Optional[BlobPrecalculateResult]:  # real-world path
				return pre_calc_result.hashes_and_chunks.pop(src_path, None)  # one-time use

			@override
			def get_previous_chunks(self, src_path: Path) -> Optional[List[PrettyChunk]]:  # real-world path
				return pre_calc_result.previous_file_chunks.get(src_path)

			@override
			def previous_backup_has_chunked_file(self, src_path: Path) -> bool:  # real-world path
				previous_file = pre_calc_result.previous_backup_files.get(file_path_to_db_path(src_path))
				return (
					previous_file is not None and
					stat.S_ISREG(previous_file.mode) and
					previous_file.blob_id is not None and
					previous_file.blob_storage_method == BlobStorageMethod.chunked.value
				)

			@override
			def is_stat_unchanged_file(self, src_path: Path) -> bool:  # real-world path
				return src_path in pre_calc_result.stat_unchanged_files

		blob_allocator = BlobAllocator(
			session=session,
			time_costs=self.__time_costs,
			blob_recorder=blob_recorder,
			source_path=self.__source_path,
			temp_path=self.__temp_path,
			file_lookup=FileLookup(),
			pack_writer=pack_writer,
		)

		self.logger.info('Scanning file for backup creation at path {!r}, targets: {}'.format(
			self.__source_path.as_posix(), self.config.backup.targets,
		))
		with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.stage_scan_files):
			scan_result = self.__scan_files()
		now_ns = time.time_ns()
		backup = session.create_backup(
			creator=str(self.creator),
			comment=self.comment,
			targets=scan_result.root_targets,
			tags=self.tags.to_dict(),
			timestamp=now_ns // (10 ** 9),
			timestamp_ns_part=now_ns % (10 ** 9),
		)
		self.logger.info('Creating backup for {} at path {!r}, file count {} size {}, timestamp {!r}, creator {!r}, comment {!r}, tags {!r}'.format(
			scan_result.root_targets, self.__source_path.as_posix(),
			len(scan_result.all_files), ByteCount(scan_result.all_file_size_sum).auto_str(),
			backup.timestamp, backup.creator, backup.comment, backup.tags,
		))

		self.__pre_calculate_stats(scan_result)
		self.__load_previous_backup_files(session)
		if self.__should_collect_stat_unchanged_files(scan_result):
			with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.stage_reuse_unchanged_files):
				self.__collect_stat_unchanged_files(scan_result)
		else:
			self.__pre_calc_result.stat_unchanged_files.clear()
		if self.config.backup.reuse_stat_unchanged_file:
			self.__reuse_unchanged_files()
			self.logger.info('Reused {} / {} stat unchanged files'.format(len(self.__pre_calc_result.reused_files), len(scan_result.all_files)))
		else:
			self.logger.debug('Found {} / {} stat unchanged files'.format(len(self.__pre_calc_result.stat_unchanged_files), len(scan_result.all_files)))
		self.__cache_previous_chunks_for_fixed_auto(session, scan_result)
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
			pack_writer.close()
			info = BackupInfo.of(backup)

		with self.__time_costs.measure_time_cost(CreateBackupTimeCostKey.stage_flush_db, CreateBackupTimeCostKey.kind_db):
			session_context.__exit__(None, None, None)
		return info

	@override
	def run(self) -> BackupInfo:
		if self.__run_called:
			raise RuntimeError('no double run')
		self.__run_called = True

		self.__time_costs.reset()
		with self.__time_costs.measure_time_cost(*CreateBackupTimeCostKey):
			pass
		action_start_ts = time.time()

		blob_recorder: Optional[BlobRecorder] = None
		try:
			session_context = DbAccess.open_session()
			with session_context as session:
				pack_writer = PackWriter(session)
				blob_recorder = BlobRecorder(pack_writer)
				info = self.__create_backup(session_context, session, pack_writer, blob_recorder)
		except Exception as e:
			if blob_recorder is not None:
				blob_recorder.apply_file_rollback()
			raise e

		bds = blob_recorder.get_blob_storage_delta()
		self.logger.info('Create backup #{} done, added {} blobs, {} chunks and {} packs (size {} / {})'.format(
			info.id, bds.blobs.count, bds.chunks.count, bds.packs.created_pack_count, ByteCount(bds.stored_size).auto_str(), ByteCount(bds.raw_size).auto_str(),
		))
		self.__log_costs(time.time() - action_start_ts)

		self.__new_blob_storage_delta = blob_recorder.get_blob_storage_delta()
		return info

	def get_new_blob_storage_delta(self) -> BlobDeltaSummary:
		return self.__new_blob_storage_delta

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
