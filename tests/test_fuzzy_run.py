import collections
import contextlib
import dataclasses
import functools
import hashlib
import os
import random
import shutil
import stat
import string
import tarfile
import time
import unittest
from io import BytesIO
from pathlib import Path
from typing import Dict, List, ContextManager, Union, Generator, Tuple, BinaryIO, Deque

from typing_extensions import Self, override

from prime_backup import logger
from prime_backup.action.create_backup_action import CreateBackupAction
from prime_backup.action.delete_backup_action import DeleteBackupAction
from prime_backup.action.delete_backup_file_action import DeleteBackupFileAction
from prime_backup.action.export_backup_action_directory import ExportBackupToDirectoryAction
from prime_backup.action.export_backup_action_tar import ExportBackupToTarAction
from prime_backup.action.get_db_overview_action import GetDbOverviewAction
from prime_backup.action.scan_unknown_blob_files import ScanUnknownBlobFilesAction
from prime_backup.action.vacuum_sqlite_action import VacuumSqliteAction
from prime_backup.action.validate_backups_action import ValidateBackupsAction
from prime_backup.action.validate_blobs_action import ValidateBlobsAction
from prime_backup.action.validate_files_action import ValidateFilesAction
from prime_backup.action.validate_filesets_action import ValidateFilesetsAction
from prime_backup.compressors import CompressMethod
from prime_backup.config.config import Config
from prime_backup.db.access import DbAccess
from prime_backup.types.hash_method import HashMethod
from prime_backup.types.operator import Operator
from prime_backup.types.tar_format import TarFormat
from prime_backup.utils import path_utils


@dataclasses.dataclass(frozen=True)
class _FileInfo:
	size: int
	sha256: str
	mode: int
	mtime: int  # timestamp in second


@dataclasses.dataclass
class _TestStats:
	file_create: int = 0
	file_append: int = 0
	file_truncate: int = 0
	file_rebuild: int = 0
	file_rewrite: int = 0
	file_delete: int = 0
	dir_create: int = 0
	dir_remove: int = 0
	backup_create: int = 0
	backup_delete: int = 0
	backup_restore: int = 0
	backup_file_delete: int = 0
	compress_method_flip: int = 0

	@classmethod
	@functools.lru_cache(None)
	def get(cls) -> Self:
		return cls()

	def reset(self) -> None:
		# noinspection PyTypeChecker
		for field in dataclasses.fields(self):
			if field.type is int:
				setattr(self, field.name, 0)


def _compute_file_sha256(file_path: Path) -> str:
	sha256_hash = hashlib.sha256()
	with open(file_path, 'rb') as f:
		while chunk := f.read(16384):
			sha256_hash.update(chunk)
	return sha256_hash.hexdigest()


@dataclasses.dataclass(frozen=True)
class Snapshot:
	files_info: Dict[Path, _FileInfo]

	@classmethod
	def from_tar(cls, tar_src: Union[Path, BinaryIO]) -> Self:
		files_info: Dict[Path, _FileInfo] = {}

		with contextlib.ExitStack() as es:
			tar: tarfile.TarFile
			if isinstance(tar_src, Path):
				tar = es.enter_context(tarfile.open(name=tar_src, mode='r:'))
			else:
				tar = es.enter_context(tarfile.open(fileobj=tar_src, mode='r:'))

			for member in tar.getmembers():
				file_path = Path(member.name)
				mode = member.mode & 0xFFFF
				if member.isfile():
					mode |= stat.S_IFREG
				elif member.isdir():
					mode |= stat.S_IFDIR
				elif member.issym():
					mode |= stat.S_IFLNK
				else:
					raise ValueError(member.type)

				if member.isfile():
					file_obj = tar.extractfile(member)
					if file_obj:
						sha256 = hashlib.sha256(file_obj.read()).hexdigest()
						files_info[file_path] = _FileInfo(
							size=member.size,
							sha256=sha256,
							mode=mode,
							mtime=member.mtime
						)
				elif member.isdir():
					files_info[file_path] = _FileInfo(
						size=0,
						sha256='',
						mode=mode,
						mtime=member.mtime
					)
		return cls(files_info)

	@classmethod
	def from_env(cls, helper: 'BackupFuzzyEnvironment') -> 'Snapshot':
		def add_path(path: Path):
			st = path.stat()
			files_info[path.relative_to(helper.snapshot_base_path)] = _FileInfo(
				size=st.st_size if path.is_file() else 0,
				sha256=_compute_file_sha256(path) if path.is_file() else '',
				mode=st.st_mode,
				mtime=int(st.st_mtime),
			)

		files_info: Dict[Path, _FileInfo] = {}
		if helper.base_path.exists():
			add_path(helper.base_path)
		for file_path in helper.get_all_dirs_and_files():
			add_path(file_path)
		return cls(files_info)


class FileSystemCache:
	def __init__(self, base_path: Path):
		self.__base_path = base_path
		self.__dirs: Dict[Path, None] = {}
		self.__files: Dict[Path, None] = {}
		self.__total_size: int = 0

	def refresh(self):
		self.__dirs.clear()
		self.__files.clear()
		self.__total_size = 0
		if not self.__base_path.exists():
			return
		if not self.__base_path.is_dir():
			raise AssertionError('base_path {} exists but is not a dir'.format(self.__base_path))
		self.__dirs[self.__base_path] = None
		for root, dirs, files in os.walk(self.__base_path):
			root_path = Path(root)
			for d in dirs:
				dir_path = root_path / d
				self.__dirs[dir_path] = None
			for f in files:
				file_path = root_path / f
				self.__files[file_path] = None
				self.__total_size += file_path.stat().st_size

	def get_all_dirs(self) -> List[Path]:
		return list(self.__dirs.keys())

	def get_all_files(self) -> List[Path]:
		return list(self.__files.keys())

	def get_total_size(self) -> int:
		return self.__total_size

	def add_file(self, file_path: Path, size: int):
		self.__files[file_path] = None
		self.__total_size += size

	def remove_file(self, file_path: Path, size: int):
		self.__files.pop(file_path)
		self.__total_size -= size

	def add_dir(self, dir_path: Path):
		self.__dirs[dir_path] = None

	def remove_dir(self, dir_path: Path):
		self.__dirs.pop(dir_path)
		for f in list(self.__files.keys()):
			if path_utils.is_relative_to(f, dir_path):
				self.__total_size -= f.stat().st_size
				self.__files.pop(f)
		for d in list(self.__dirs.keys()):
			if path_utils.is_relative_to(d, dir_path):
				self.__dirs.pop(d)

	def adjust_total_size(self, delta: int):
		self.__total_size += delta

	def has_file(self, file_path: Path) -> bool:
		return file_path in self.__files

	def has_dir(self, new_dir: Path) -> bool:
		return new_dir in self.__dirs

	def get_summary_text(self) -> str:
		return f'dirs: {len(self.__dirs)}, files: {len(self.__files)}, size: {self.__total_size}'


class FileContentGenerator:
	MAX_CACHED_SIZE = 30 * 1024 * 1024
	REUSE_PROBABILITY = 0.1

	def __init__(self, rnd: random.Random):
		self.rnd = rnd
		self.__cache: Dict[int, List[bytes]] = collections.defaultdict(list)
		self.__keys: Deque[int] = collections.deque()
		self.__total_size = 0

	def generate(self, min_size: int, max_size: int) -> bytes:
		if self.__cache and self.rnd.random() < self.REUSE_PROBABILITY:
			key = self.rnd.choice(self.__keys)
			return self.rnd.choice(self.__cache[key])

		size = self.rnd.randint(min_size, self.rnd.randint(min_size, max_size))
		buf = self.rnd.randbytes(size)

		def try_insert() -> bool:
			if self.__total_size < self.MAX_CACHED_SIZE:
				self.__cache[size].append(buf)
				self.__keys.append(size)
				self.__total_size += size
				return True
			return False

		first_attempt_ok = try_insert()
		if not first_attempt_ok and self.rnd.random() < 0.1 and len(self.__keys) > 0:
			old_key = self.__keys.popleft() if self.rnd.random() < 0.5 else self.__keys.pop()
			old_buf_list = self.__cache[old_key]
			old_len = len(old_buf_list[0])
			old_buf_list.pop()
			self.__total_size -= old_len
			if len(old_buf_list) == 0:
				self.__cache.pop(old_key)
			try_insert()

		return buf


class BackupFuzzyEnvironment(ContextManager['BackupFuzzyEnvironment']):
	MAX_DEPTH: int = 5
	MAX_TOTAL_SIZE_MB: int = 100

	def __init__(self, test: unittest.TestCase, base_path: Path, snapshot_base_path: Path, rnd: random.Random) -> None:
		self.test = test
		self.base_path = base_path  # might not exists
		self.snapshot_base_path = snapshot_base_path
		self.rnd = rnd
		self.logger = logger.get()
		self.__fs_cache = FileSystemCache(self.base_path)
		self.__file_gen = FileContentGenerator(rnd)

	def create(self) -> None:
		if self.base_path.exists():
			shutil.rmtree(self.base_path)
		self.base_path.mkdir(parents=True)
		self.__fs_cache.refresh()

		for _ in range(self.rnd.randint(5, 10)):
			self.__create_random_file_at(self.base_path)

	def destroy(self) -> None:
		if self.base_path.is_dir():
			shutil.rmtree(self.base_path)

	def __enter__(self):
		self.create()
		return self

	def __exit__(self, exc_type, exc_value, traceback, /):
		self.destroy()

	def create_snapshot(self) -> Snapshot:
		return Snapshot.from_env(self)

	def iterate_once(self) -> None:
		if not self.base_path.exists():
			self.create()

		delete_chance = self.rnd.random() % 0.001
		modify_chance = self.rnd.random() % 0.1  # [0, 0.1)
		rmdir_chance = self.rnd.random() % 0.001

		# Handle file deletions
		for file_path in self.__fs_cache.get_all_files():
			if self.rnd.random() < delete_chance:
				self.__remove_file(file_path)

		# Handle file modifications
		for file_path in self.__fs_cache.get_all_files():
			if self.rnd.random() < modify_chance:
				self.__modify_file(file_path)

		# Handle directory deletions
		for dir_path in self.__fs_cache.get_all_dirs():
			if dir_path != self.base_path and self.rnd.random() < rmdir_chance and dir_path.exists():
				self.__remove_dir(dir_path)

		# Add new items (files or directories)
		all_dirs = self.__fs_cache.get_all_dirs()
		num_new_items: int = self.rnd.randint(0, 10)
		for _ in range(num_new_items):
			if self.__fs_cache.get_total_size() >= self.MAX_TOTAL_SIZE_MB * 1024 * 1024:
				break

			target_dir: Path = self.rnd.choice(all_dirs)
			if self.rnd.random() < 0.3 and self.__get_depth(target_dir) < self.MAX_DEPTH:
				self.__create_dir(target_dir / self.__random_string(5))
			else:
				self.__create_random_file_at(target_dir)

	def get_all_dirs_and_files(self) -> List[Path]:
		return [
			*self.__fs_cache.get_all_dirs(),
			*self.__fs_cache.get_all_files(),
		]

	def __create_random_file_at(self, directory: Path) -> None:
		file_name: str = f'{self.__random_string(8)}.{self.rnd.choice(["txt", "bin", "dat"])}'
		self.__create_file(directory / file_name)

	def __create_file(self, file_path: Path):
		if self.__fs_cache.has_file(file_path):
			return

		file_content = self.__file_gen.generate(0, self.rnd.randint(0, 1024 * 100))
		size = len(file_content)
		self.logger.info(f'ENV: create file {file_path}, size: {size}')
		with open(file_path, 'wb') as f:
			f.write(file_content)
		random_time: float = time.time() - self.rnd.randint(0, 30 * 24 * 3600)
		os.utime(file_path, (random_time, random_time))
		self.__fs_cache.add_file(file_path, size)
		_TestStats.get().file_create += 1

	def __modify_file(self, file_path: Path) -> None:
		old_size = file_path.stat().st_size
		mod_type: str = self.rnd.choice(['append', 'truncate', 'rewrite', 'rebuild'])
		self.logger.info(f'ENV: modify file {file_path}, mod_type: {mod_type}')
		if mod_type == 'append':
			with open(file_path, 'ab') as f:
				f.write(self.rnd.randbytes(self.rnd.randint(512, 1024 * 10)))
			_TestStats.get().file_append += 1
		elif mod_type == 'truncate':
			current_size: int = file_path.stat().st_size
			if current_size > 1024:
				with open(file_path, 'ab') as f:
					f.truncate(self.rnd.randint(512, current_size - 512))
			_TestStats.get().file_truncate += 1
		else:  # rewrite / rebuild
			if mod_type == 'rebuild':
				file_path.unlink()
				_TestStats.get().file_rebuild += 1
			else:
				_TestStats.get().file_rewrite += 1
			file_content = self.__file_gen.generate(0, self.rnd.randint(0, 1024 * 100))
			with open(file_path, 'wb') as f:
				f.write(file_content)
		random_time: float = time.time() - self.rnd.randint(0, 7 * 24 * 3600)
		os.utime(file_path, (random_time, random_time))
		new_size = file_path.stat().st_size
		self.__fs_cache.adjust_total_size(new_size - old_size)

	def __remove_file(self, file_path: Path):
		self.logger.info(f'ENV: remove file {file_path}')
		self.__fs_cache.remove_file(file_path, file_path.stat().st_size)
		file_path.unlink()
		_TestStats.get().file_delete += 1

	def __create_dir(self, dir_path: Path):
		if dir_path.exists() or self.__fs_cache.has_dir(dir_path):
			return
		self.logger.info(f'ENV: create dir {dir_path}')
		dir_path.mkdir()
		self.__fs_cache.add_dir(dir_path)
		_TestStats.get().dir_create += 1

	def __remove_dir(self, dir_path: Path):
		self.logger.info(f'ENV: create dir {dir_path}')
		self.__fs_cache.remove_dir(dir_path)
		shutil.rmtree(dir_path)
		_TestStats.get().dir_remove += 1

	def __random_string(self, length: int) -> str:
		return ''.join(self.rnd.choices(string.ascii_lowercase + string.digits, k=length))

	def __get_depth(self, path: Path) -> int:
		return len(path.relative_to(self.base_path).parts)

	def refresh_fs_cache(self):
		self.__fs_cache.refresh()

	def get_fs_summary_text(self) -> str:
		return self.__fs_cache.get_summary_text()


class FuzzyRunTestCase(unittest.TestCase):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.logger = logger.get()

	@override
	def setUp(self):
		_TestStats.get().reset()

		from prime_backup.action.helpers.fileset_allocator import FilesetAllocateArgsDefaults
		FilesetAllocateArgsDefaults.candidate_max_changes_ratio = 0.4  # increase this for easier fileset reuse

	@contextlib.contextmanager
	def create_env(self, rnd: random.Random) -> Generator[Tuple[BackupFuzzyEnvironment, Path, Path], None, None]:
		test_root = Path(os.environ.get('PRIME_BACKUP_FUZZY_TEST_ROOT', 'run/unittest'))
		pb_dir = test_root / 'pb_files'
		fake_server_dir = test_root / 'server'
		env_dir = fake_server_dir / 'world'
		temp_dir = test_root / 'temp'
		gitignore_file = test_root / '.gitignore'

		def rm_test_files_dirs():
			for d in [pb_dir, env_dir, temp_dir]:
				if d.is_dir():
					shutil.rmtree(d)

		rm_test_files_dirs()
		test_root.mkdir(parents=True, exist_ok=True)
		gitignore_file.write_text('**\n', encoding='utf8')

		Config.get().storage_root = str(pb_dir)
		Config.get().backup.source_root = str(fake_server_dir)
		Config.get().backup.targets = [env_dir.name]
		Config.get().backup.hash_method = HashMethod.xxh128
		Config.get().backup.compress_method = CompressMethod.plain
		DbAccess.init(create=True, migrate=False)

		with contextlib.ExitStack() as es:
			if os.environ.get('PRIME_BACKUP_FUZZY_TEST_KEEP', '').lower() not in ('true', '1'):
				es.callback(rm_test_files_dirs)
			es.callback(DbAccess.shutdown)
			env = es.enter_context(BackupFuzzyEnvironment(self, env_dir, fake_server_dir, rnd))
			yield env, fake_server_dir, temp_dir

	def test_fuzzy_run(self):
		env: BackupFuzzyEnvironment
		svr_dir: Path
		temp_dir: Path

		seed = int(os.environ.get('PRIME_BACKUP_FUZZY_TEST_SEED', '0'))
		iterations = int(os.environ.get('PRIME_BACKUP_FUZZY_TEST_ITERATION', '200'))
		self.logger.info(f'Random seed: {seed}')
		self.logger.info(f'Iterations: {iterations}')
		rnd = random.Random(seed)
		with self.create_env(rnd) as (env, svr_dir, temp_dir):
			def create_backup() -> int:
				_TestStats.get().backup_create += 1
				return CreateBackupAction(Operator.literal('test'), '').run().id

			def delete_backup(bid_: int):
				_TestStats.get().backup_delete += 1
				DeleteBackupAction(bid_).run()

			def restore_backup(bid_: int):
				_TestStats.get().backup_restore += 1
				ExportBackupToDirectoryAction(bid_, svr_dir, restore_mode=True).run()
				env.refresh_fs_cache()

			def delete_backup_file(bid_: int, mem_snapshot: Snapshot):
				paths = list(mem_snapshot.files_info.keys())
				if len(paths) == 0:
					return
				path_to_delete = rnd.choice(paths)
				for path in paths:
					if path_utils.is_relative_to(path, path_to_delete):
						mem_snapshot.files_info.pop(path)
						_TestStats.get().backup_file_delete += 1
				DeleteBackupFileAction(bid_, path_to_delete).run()

			def get_backup_snapshot(bid_: int) -> Snapshot:
				buf = BytesIO()
				ExportBackupToTarAction(bid_, buf, TarFormat.plain, create_meta=False).run()
				buf.seek(0)
				return Snapshot.from_tar(buf)

			def validate_all():
				# Verify all existing backups remain unchanged
				for bid in backup_ids:
					current_snapshot = get_backup_snapshot(bid)
					initial_snapshot = backup_snapshots[bid]
					self.assertEqual(initial_snapshot, current_snapshot, f'Backup {bid} data changed at iteration {i}')

				# Validate database
				r0 = ValidateBlobsAction().run()
				r1 = ValidateFilesAction().run()
				r2 = ValidateFilesetsAction().run()
				r3 = ValidateBackupsAction().run()
				try:
					self.assertEqual(0, r0.bad, f'ValidateBlobsAction has bad: {r0}')
					self.assertEqual(0, r1.bad, f'ValidateFilesAction has bad: {r1}')
					self.assertEqual(0, r2.bad, f'ValidateFilesetsAction has bad: {r2}')
					self.assertEqual(0, r3.bad, f'ValidateBackupsAction has bad: {r3}')
				except AssertionError as e:
					self.logger.error(f'Validate DB failed: {e}')
					self.logger.error(repr(r0))
					self.logger.error(repr(r1))
					self.logger.error(repr(r2))
					self.logger.error(repr(r3))
					raise

				# check dangling / unused stuffs
				r_ub = ScanUnknownBlobFilesAction(delete=False, result_sample_limit=100).run()
				for ub in r_ub.samples:
					self.logger.info(str(ub))
				self.assertEqual(0, r_ub.count, 'ScanUnknownBlobFilesAction found {} unknown blobs'.format(r_ub.count))

				# tidying
				VacuumSqliteAction().run()

			backup_snapshots: Dict[int, Snapshot] = {}
			backup_ids: List[int] = []

			self.logger.info('Fuzzy test start')
			for i in range(iterations):
				self.logger.info(f'============================== Iteration {i} ==============================')

				# Step 1: Create new backups with 50% probability
				if rnd.random() < 0.7:
					backup_num = 1
					if rnd.random() < 0.01:  # 2% chance for another backup
						backup_num += 1

					for _ in range(backup_num):
						snapshot_before = env.create_snapshot()
						backup_id = create_backup()
						backup_ids.append(backup_id)

						# Verify backup creation doesn't modify environment
						snapshot_after = env.create_snapshot()
						self.assertEqual(snapshot_before, snapshot_after, f'Backup creation modified env at iteration {i} for backup {backup_id}')

						# Verify backup snapshot matches environment
						backup_snapshot = get_backup_snapshot(backup_id)
						self.assertEqual(snapshot_before, backup_snapshot, f'Backup {backup_id} snapshot mismatch with env at iteration {i}')

						# Store initial snapshot for later verification
						backup_snapshots[backup_id] = backup_snapshot

				# Step 2: Restore a random backup with 5% probability (if any exist)
				if backup_ids and rnd.random() < 0.05:
					restore_id = rnd.choice(backup_ids)
					restore_backup(restore_id)
					snapshot_after_restore = env.create_snapshot()
					restored_backup_snapshot = backup_snapshots[restore_id]
					self.assertEqual(snapshot_after_restore, restored_backup_snapshot, f'Restore of backup {restore_id} mismatch at iteration {i}')

				# Step 3: Delete a random backup with 20% probability (if any exist)
				if backup_ids and rnd.random() < 0.2:
					chosen_deleted_id = rnd.choice(backup_ids)
					for delete_id in backup_ids[:]:
						if delete_id == chosen_deleted_id or rnd.random() < 0.01:
							delete_backup(delete_id)
							backup_ids.remove(delete_id)
							backup_snapshots.pop(delete_id)
				if backup_ids and rnd.random() < 0.2:
					to_mess_backup_id = rnd.choice(backup_ids)
					delete_backup_file(to_mess_backup_id, backup_snapshots[to_mess_backup_id])

				# Step 4: Modify environment
				env.iterate_once()

				# Step 5: Config alternation
				if rnd.random() < 0.01:
					new_compress_method = rnd.choice([CompressMethod.plain, CompressMethod.zstd])
					self.logger.info('Changing compress method {} -> {}'.format(Config.get().backup.compress_method, new_compress_method))
					Config.get().backup.compress_method = new_compress_method
					_TestStats.get().compress_method_flip += 1

				# Step 6: Validate all
				if i % 50 == 0 or i == iterations - 1:
					self.logger.info('Validating everything at iteration {}'.format(i))
					validate_all()

				# Step 7: Show summary
				self.logger.info('Iteration {} done'.format(i))
				self.logger.info('Test stats: {}'.format(_TestStats.get()))
				self.logger.info('DB: {}'.format(GetDbOverviewAction().run()))
				self.logger.info('ENV: {}'.format(env.get_fs_summary_text()))

			# Final deletion test
			self.logger.info(f'============================== Final Deletion Test ==============================')
			rnd.shuffle(backup_ids)
			backup_num = len(backup_ids)
			step = max(5.0, backup_num * 0.05)  # every 5%
			self.logger.info('Starting the final deletion test with backup_num={} and step={:.2f}'.format(backup_num, step))

			next_check = step
			for i, backup_id in enumerate(backup_ids.copy()):
				self.logger.info('Deleting backup {}'.format(backup_id))
				delete_backup(backup_id)
				backup_ids.remove(backup_id)
				backup_snapshots.pop(backup_id)

				idx = i + 1
				if idx >= next_check - 1e-8:
					self.logger.info(f'Validating everything, {step=:.2f} {idx=:.2f} {next_check=:.2f}')
					next_check += step
					validate_all()

			db_overview = GetDbOverviewAction().run()
			self.logger.info(f'Checking if the database is empty: {db_overview}')
			self.assertEqual(0, db_overview.blob_count)
			self.assertEqual(0, db_overview.file_object_count)
			self.assertEqual(0, db_overview.file_total_count)
			self.assertEqual(0, db_overview.fileset_count)
			self.assertEqual(0, db_overview.backup_count)
			self.assertEqual(0, db_overview.blob_stored_size_sum)
			self.assertEqual(0, db_overview.blob_raw_size_sum)
			self.assertEqual(0, db_overview.file_raw_size_sum)

			self.logger.info('Fuzzy test passed')
			self.logger.info('Final test stats: {}'.format(_TestStats.get()))


if __name__ == '__main__':
	unittest.main()
