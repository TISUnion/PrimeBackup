import dataclasses
import functools
from abc import ABC
from pathlib import Path
from typing import List, Callable, Optional, Dict, Tuple, Iterable

from prime_backup import logger
from prime_backup.action import Action
from prime_backup.db import schema
from prime_backup.db.schema import FileRole
from prime_backup.db.session import DbSession
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.blob_info import BlobInfo, BlobListSummary


def _sum_file_sizes(files: Iterable[schema.File]) -> Tuple[int, int]:
	file_raw_size_sum = 0
	file_stored_size_sum = 0

	for f in files:
		if f.blob_raw_size is not None:
			file_raw_size_sum += f.blob_raw_size
		if f.blob_stored_size is not None:
			file_stored_size_sum += f.blob_stored_size

	return file_raw_size_sum, file_stored_size_sum


class _FilesetAllocator:
	def __init__(self, session: DbSession, files: List[schema.File]):
		self.session = session
		self.files = files

	@dataclasses.dataclass(frozen=True)
	class Delta:
		@dataclasses.dataclass(frozen=True)
		class OldNewFile:
			old: schema.File
			new: schema.File

		added: List[schema.File] = dataclasses.field(default_factory=list)  # list of new
		removed: List[schema.File] = dataclasses.field(default_factory=list)  # list of old
		changed: List[OldNewFile] = dataclasses.field(default_factory=list)  # list of (old, new)

		def size(self) -> int:
			return len(self.added) + len(self.removed) + len(self.changed)

	@dataclasses.dataclass(frozen=True)
	class SelectResult:
		fileset_base: schema.Fileset
		fileset_delta: schema.Fileset

	@classmethod
	def __get_file_by_path(cls, files: List[schema.File]) -> Dict[str, schema.File]:
		return {f.path: f for f in files}

	@classmethod
	def __is_file_equaled(cls, a: schema.File, b: schema.File):
		# TODO: discard atime?
		return (
			a.path == b.path and a.mode == b.mode and
			a.content == b.content and a.blob_hash == b.blob_hash and
			a.uid == b.uid and a.gid == b.gid and
			a.ctime_ns == b.ctime_ns and a.mtime_ns == b.mtime_ns
		)

	@classmethod
	def __calc_delta(cls, old: Dict[str, schema.File], new: Dict[str, schema.File]) -> Delta:
		delta = cls.Delta()
		for path, old_file in old.items():
			if path in new:
				new_file = new[path]
				if not cls.__is_file_equaled(new_file, old_file):
					delta.changed.append(cls.Delta.OldNewFile(old_file, new_file))
			else:
				delta.removed.append(old_file)
		for path in new.keys():
			if path not in old:
				delta.added.append(new[path])
		return delta

	def calc(self, max_changes_ratio: float, last_n: int) -> SelectResult:
		@dataclasses.dataclass(frozen=True)
		class Candidate:
			fileset: schema.Fileset
			file_by_path: Dict[str, schema.File]
			delta: _FilesetAllocator.Delta
			delta_size: int

		c: Optional[Candidate] = None
		file_by_path = self.__get_file_by_path(self.files)

		for c_fileset in self.session.get_last_n_base_fileset(limit=last_n):
			c_fileset_files = self.session.get_fileset_files(c_fileset.id)
			c_file_by_path = self.__get_file_by_path(c_fileset_files)
			delta = self.__calc_delta(c_file_by_path, file_by_path)
			logger.get().info('FILESET allocate candidate {} {}'.format(c_fileset.id, delta.size()))
			if delta.size() < len(file_by_path) * max_changes_ratio and (c is None or delta.size() < c.delta_size):
				c = Candidate(c_fileset, c_file_by_path, delta, delta.size())

		logger.get().info('FILESET allocate candidate decided {}'.format(c.fileset.id))
		if c is None:
			# create a new base fileset
			fileset_base = self.session.create_and_add_fileset(base=True)
			fileset_delta = self.session.create_and_add_fileset(base=False)
			self.session.flush()

			for file in self.files:
				file.fileset_id = fileset_base.id
				file.role = FileRole.standalone.value
				self.session.add(file)

			fileset_base.file_raw_size_sum, fileset_base.file_stored_size_sum = _sum_file_sizes(self.files)
			fileset_delta.file_raw_size_sum, fileset_delta.file_stored_size_sum = 0, 0
			return self.SelectResult(fileset_base, fileset_delta)
		else:
			# reuse the existing base fileset
			fileset_delta = self.session.create_and_add_fileset(base=False)
			self.session.flush()

			delta_files: List[schema.File] = []

			# these sum are deltas
			file_raw_size_sum = 0
			file_stored_size_sum = 0
			for new_file in c.delta.added:
				new_file.fileset_id = fileset_delta.id
				new_file.role = FileRole.delta_add.value
				file_raw_size_sum += new_file.blob_raw_size
				file_stored_size_sum += new_file.blob_stored_size
				delta_files.append(new_file)

			for old_new in c.delta.changed:
				old_new.new.fileset_id = fileset_delta.id
				old_new.new.role = FileRole.delta_override.value
				file_raw_size_sum += old_new.new.blob_raw_size - old_new.old.blob_raw_size
				file_stored_size_sum += old_new.old.blob_stored_size - old_new.old.blob_stored_size
				delta_files.append(old_new.new)

			for old_file in c.delta.removed:
				file = self.session.create_file(
					path=old_file.path,
					fileset_id=fileset_delta.id,
					role=FileRole.delta_remove.value,
					mode=0,
				)
				self.session.add(file)
				file_raw_size_sum -= old_file.blob_raw_size
				file_stored_size_sum -= old_file.blob_stored_size
				delta_files.append(file)

			for file in delta_files:
				self.session.add(file)

			fileset_delta.file_raw_size_sum, fileset_delta.file_stored_size_sum = file_raw_size_sum, file_stored_size_sum
			return self.SelectResult(c.fileset, fileset_delta)


class CreateBackupActionBase(Action[BackupInfo], ABC):
	def __init__(self):
		super().__init__()
		self.__new_blobs: List[BlobInfo] = []
		self.__new_blobs_summary: Optional[BlobListSummary] = None
		self.__blobs_rollbackers: List[Callable] = []

	def _remove_file(self, file_to_remove: Path, *, what: str = 'rollback'):
		try:
			file_to_remove.unlink(missing_ok=True)
		except OSError as e:
			self.logger.error('({}) remove file {!r} failed: {}'.format(what, file_to_remove, e))

	def _add_remove_file_rollbacker(self, file_to_remove: Path):
		self.__blobs_rollbackers.append(functools.partial(self._remove_file, file_to_remove=file_to_remove))

	def _apply_blob_rollback(self):
		if len(self.__blobs_rollbackers) > 0:
			self.logger.warning('Error occurs during backup creation, applying rollback')
			for rollback_func in self.__blobs_rollbackers:
				rollback_func()
			self.__blobs_rollbackers.clear()

	def _create_blob(self, session: DbSession, **kwargs) -> schema.Blob:
		blob = session.create_and_add_blob(**kwargs)
		self.__new_blobs.append(BlobInfo.of(blob))
		return blob

	def get_new_blobs_summary(self) -> BlobListSummary:
		if self.__new_blobs_summary is None:
			self.__new_blobs_summary = BlobListSummary.of(self.__new_blobs)
		return self.__new_blobs_summary

	@classmethod
	def _finalize_backup_and_files(cls, session: DbSession, backup: schema.Backup, files: List[schema.File]):
		allocator = _FilesetAllocator(session, files)
		alloc_result = allocator.calc(max_changes_ratio=0.2, last_n=3)

		fs_base, fs_delta = alloc_result.fileset_base, alloc_result.fileset_delta

		backup.fileset_id_base = fs_base.id
		backup.fileset_id_delta = fs_delta.id
		backup.file_raw_size_sum = fs_base.file_raw_size_sum + fs_delta.file_raw_size_sum
		backup.file_stored_size_sum = fs_base.file_stored_size_sum + fs_delta.file_stored_size_sum

		session.add(backup)
		session.flush()  # generates backup.id

	def run(self) -> None:
		self.__new_blobs.clear()
		self.__new_blobs_summary = None
		self.__blobs_rollbackers.clear()
