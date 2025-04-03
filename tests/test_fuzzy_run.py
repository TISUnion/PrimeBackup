import contextlib
import hashlib
import os
import random
import shutil
import stat
import string
import tarfile
import time
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Set, List, ContextManager, Union, Generator, Tuple
from unittest import TestCase

from typing_extensions import override

from prime_backup.action.create_backup_action import CreateBackupAction
from prime_backup.action.delete_backup_action import DeleteBackupAction
from prime_backup.action.export_backup_action_directory import ExportBackupToDirectoryAction
from prime_backup.action.export_backup_action_tar import ExportBackupToTarAction
from prime_backup.action.get_db_overview_action import GetDbOverviewAction
from prime_backup.config.config import Config
from prime_backup.db.access import DbAccess
from prime_backup.types.operator import Operator
from prime_backup.types.tar_format import TarFormat


@dataclass(frozen=True)
class FileInfo:
	size: int
	sha256: str
	mode: int
	mtime: int  # timestamp in second


def _compute_file_sha256(file_path: Path) -> str:
	sha256_hash = hashlib.sha256()
	with open(file_path, 'rb') as f:
		while chunk := f.read(4096):
			sha256_hash.update(chunk)
	return sha256_hash.hexdigest()


@dataclass(frozen=True)
class Snapshot:
	files_info: Dict[Path, FileInfo]

	@classmethod
	def from_tar(cls, tar_path: Union[str, Path]) -> 'Snapshot':
		files_info: Dict[Path, FileInfo] = {}

		with tarfile.open(tar_path, 'r') as tar:
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
						files_info[file_path] = FileInfo(
							size=member.size,
							sha256=sha256,
							mode=mode,
							mtime=member.mtime
						)
				elif member.isdir():
					files_info[file_path] = FileInfo(
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
			files_info[path.relative_to(helper.snapshot_base_path)] = FileInfo(
				size=st.st_size if path.is_file() else 0,
				sha256=_compute_file_sha256(path) if path.is_file() else '',
				mode=st.st_mode,
				mtime=int(st.st_mtime),
			)

		files_info: Dict[Path, FileInfo] = {}
		add_path(helper.base_path)
		for file_path in helper.get_all_dirs_and_files():
			add_path(file_path)
		return cls(files_info)

	def compute_diff(self, other: 'Snapshot') -> Dict[str, Set[Path]]:
		diff: Dict[str, Set[Path]] = {'added': set(), 'removed': set(), 'modified': set()}
		self_paths: Set[Path] = set(self.files_info.keys())
		other_paths: Set[Path] = set(other.files_info.keys())
		diff['added'] = other_paths - self_paths
		diff['removed'] = self_paths - other_paths
		for path in self_paths & other_paths:
			if self.files_info[path] != other.files_info[path]:
				diff['modified'].add(path)
		return diff


class BackupFuzzyEnvironment(ContextManager['BackupFuzzyEnvironment']):
	MAX_FILES_PER_DIR: int = 100
	MAX_DEPTH: int = 3
	MAX_TOTAL_SIZE_MB: int = 100

	def __init__(self, test: unittest.TestCase, base_path: Path, snapshot_base_path: Path, rnd: random.Random) -> None:
		self.test = test
		self.base_path = base_path
		self.snapshot_base_path = snapshot_base_path
		self.rnd = rnd

	def create(self) -> None:
		if self.base_path.exists():
			shutil.rmtree(self.base_path)
		self.base_path.mkdir(parents=True)
		for _ in range(self.rnd.randint(5, 10)):
			self._create_random_file(self.base_path)

	def destroy(self) -> None:
		if self.base_path.is_dir():
			shutil.rmtree(self.base_path)

	@override
	def __enter__(self):
		self.create()
		return self

	@override
	def __exit__(self, exc_type, exc_value, traceback, /):
		self.destroy()

	def create_snapshot(self) -> Snapshot:
		return Snapshot.from_env(self)

	def iterate_once(self) -> None:
		all_dirs: List[Path] = self._get_all_dirs()
		all_files: List[Path] = self._get_all_files()
		print(f'{len(all_dirs)=} {len(all_files)=}')

		for file_path in all_files[:]:
			if self.rnd.random() < (1.0 / 1000):  # keep ~1k files
				file_path.unlink()
				all_files.remove(file_path)

		for file_path in all_files[:]:
			if self.rnd.random() < 0.1:
				self._modify_file(file_path)

		for dir_path in all_dirs[:]:
			if dir_path != self.base_path and self.rnd.random() < 0.001:
				shutil.rmtree(dir_path, ignore_errors=True)
				all_dirs.remove(dir_path)

		all_dirs: List[Path] = self._get_all_dirs()
		num_new_items: int = self.rnd.randint(1, 10)
		for _ in range(num_new_items):
			if self._get_total_size() >= self.MAX_TOTAL_SIZE_MB * 1024 * 1024:
				break
			target_dir: Path = self.rnd.choice(all_dirs)
			if self.rnd.random() < 0.3 and self._get_depth(target_dir) < self.MAX_DEPTH:
				new_dir: Path = target_dir / self._random_string(5)
				if not new_dir.exists():
					new_dir.mkdir()
					all_dirs.append(new_dir)
			else:
				if len(list(target_dir.iterdir())) < self.MAX_FILES_PER_DIR:
					self._create_random_file(target_dir)

	def _get_all_dirs(self) -> List[Path]:
		dirs: List[Path] = [self.base_path]
		for root, subdirs, _ in os.walk(self.base_path):
			for subdir in subdirs:
				dirs.append(Path(root) / subdir)
		return dirs

	def _get_all_files(self) -> List[Path]:
		files: List[Path] = []
		for root, _, filenames in os.walk(self.base_path):
			for filename in filenames:
				files.append(Path(root) / filename)
		return files

	def get_all_dirs_and_files(self) -> List[Path]:
		files: List[Path] = []
		for root, subdirs, filenames in os.walk(self.base_path):
			for subdir in subdirs:
				files.append(Path(root) / subdir)
			for filename in filenames:
				files.append(Path(root) / filename)
		return files

	def _get_total_size(self) -> int:
		total_size: int = 0
		for file_path in self._get_all_files():
			total_size += file_path.stat().st_size
		return total_size

	def _create_random_file(self, directory: Path) -> None:
		file_name: str = f'{self._random_string(8)}.{self.rnd.choice(["txt", "bin", "dat"])}'
		file_path: Path = directory / file_name
		size: int = self.rnd.randint(1024, 1024 * 100)
		with open(file_path, 'wb') as f:
			f.write(self.rnd.randbytes(size))
		random_time: float = time.time() - self.rnd.randint(0, 30 * 24 * 3600)
		os.utime(file_path, (random_time, random_time))

	def _modify_file(self, file_path: Path) -> None:
		mod_type: str = self.rnd.choice(['append', 'truncate', 'rewrite'])
		if mod_type == 'append':
			with open(file_path, 'ab') as f:
				f.write(self.rnd.randbytes(self.rnd.randint(512, 1024 * 10)))
		elif mod_type == 'truncate':
			current_size: int = file_path.stat().st_size
			if current_size > 1024:
				with open(file_path, 'ab') as f:
					f.truncate(self.rnd.randint(512, current_size - 512))
		else:
			with open(file_path, 'wb') as f:
				f.write(self.rnd.randbytes(self.rnd.randint(1024, 1024 * 50)))
		random_time: float = time.time() - self.rnd.randint(0, 7 * 24 * 3600)
		os.utime(file_path, (random_time, random_time))

	def _random_string(self, length: int) -> str:
		return ''.join(self.rnd.choices(string.ascii_lowercase + string.digits, k=length))

	def _get_depth(self, path: Path) -> int:
		return len(path.relative_to(self.base_path).parts)


class FuzzyRunTestCase(TestCase):
	@contextlib.contextmanager
	def create_env(self, rnd: random.Random) -> Generator[Tuple[BackupFuzzyEnvironment, Path, Path], None, None]:
		test_root = Path('run') / 'unittest'
		pb_dir = test_root / 'pb_files'
		fake_server_dir = test_root / 'server'
		env_dir = fake_server_dir / 'world'
		temp_dir = test_root / 'temp'

		def rm_test_dirs():
			for d in [pb_dir, env_dir, temp_dir]:
				if d.is_dir():
					shutil.rmtree(d)

		rm_test_dirs()

		Config.get().storage_root = str(pb_dir)
		Config.get().backup.source_root = str(fake_server_dir)
		Config.get().backup.targets = [env_dir.name]
		DbAccess.init(create=True, migrate=False)

		with contextlib.ExitStack() as es:
			es.callback(rm_test_dirs)
			es.callback(DbAccess.shutdown)
			env = es.enter_context(BackupFuzzyEnvironment(self, env_dir, fake_server_dir, rnd))
			yield env, fake_server_dir, temp_dir

	FUZZY_ITERATIONS: int = 1000

	def test_fuzzy_run(self):
		env: BackupFuzzyEnvironment
		temp_dir: Path
		rnd = random.Random(123)
		with self.create_env(rnd) as (env, svr_dir, temp_dir):
			def create_backup() -> int:
				return CreateBackupAction(Operator.literal('test'), '').run().id

			def delete_backup(bid_: int):
				DeleteBackupAction(bid_).run()

			def restore_backup(bid_: int):
				ExportBackupToDirectoryAction(bid_, svr_dir, restore_mode=True).run()

			def get_backup_snapshot(bid_: int) -> Snapshot:
				tar_path = temp_dir / '1.tar'
				ExportBackupToTarAction(bid_, tar_path, TarFormat.plain, create_meta=False).run()
				return Snapshot.from_tar(tar_path)

			backup_snapshots: Dict[int, Snapshot] = {}
			backup_ids: List[int] = []

			for i in range(self.FUZZY_ITERATIONS):
				# Step 1: Show info
				print(f'============================== Iteration {i} ==============================')
				print(GetDbOverviewAction().run())

				# Step 2: Create a new backup with 50% probability
				if rnd.random() < 0.7:
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

				# Step 3: Restore a random backup with 5% probability (if any exist)
				if backup_ids and rnd.random() < 0.05:
					restore_id = rnd.choice(backup_ids)
					restore_backup(restore_id)
					snapshot_after_restore = env.create_snapshot()
					restored_backup_snapshot = backup_snapshots[restore_id]
					self.assertEqual(snapshot_after_restore, restored_backup_snapshot, f'Restore of backup {restore_id} mismatch at iteration {i}')

				# Step 4: Delete a random backup with 20% probability (if any exist)
				if backup_ids and rnd.random() < 0.2:
					for delete_id in backup_ids[:]:
						if rnd.random() < 0.05:
							delete_backup(delete_id)
							backup_ids.remove(delete_id)
							backup_snapshots.pop(delete_id)

				# Step 5: Modify environment
				env.iterate_once()

			# Verify all existing backups remain unchanged
			for bid in backup_ids:
				current_snapshot = get_backup_snapshot(bid)
				initial_snapshot = backup_snapshots[bid]
				self.assertEqual(initial_snapshot, current_snapshot, f'Backup {bid} data changed at iteration {i}')


if __name__ == '__main__':
	unittest.main()
