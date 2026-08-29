import dataclasses
import os
import stat
import threading
from pathlib import Path
from typing import Callable, Dict, List, Optional

from prime_backup import logger
from prime_backup.db import schema
from prime_backup.utils import hash_utils


class RestoreFileReuseRollbackError(RuntimeError):
	pass


def _i_am_root() -> bool:
	# reference: tarfile.TarFile.chown
	return hasattr(os, 'geteuid') and os.geteuid() == 0


@dataclasses.dataclass(frozen=True)
class _DirectoryTimes:
	atime_ns: int
	mtime_ns: int

	@classmethod
	def read(cls, path: Path) -> '_DirectoryTimes':
		st = path.lstat()
		return cls(st.st_atime_ns, st.st_mtime_ns)

	def apply(self, path: Path):
		os.utime(path, ns=(self.atime_ns, self.mtime_ns))


@dataclasses.dataclass(frozen=True)
class _FileAttrs:
	mode: int
	uid: int
	gid: int
	atime_ns: int
	mtime_ns: int

	@classmethod
	def of_stat(cls, st: os.stat_result) -> '_FileAttrs':
		return cls(st.st_mode, st.st_uid, st.st_gid, st.st_atime_ns, st.st_mtime_ns)

	def apply(self, path: Path):
		if _i_am_root():
			os.chown(path, self.uid, self.gid)
		os.chmod(path, self.mode)
		os.utime(path, ns=(self.atime_ns, self.mtime_ns))


@dataclasses.dataclass(frozen=True)
class _ReuseCandidate:
	file: schema.File
	relative_path: Path
	trash_path: Path
	destination_path: Path
	original_attrs: _FileAttrs
	metadata_matches: bool


class RestoreFileReuser:
	def __init__(
			self,
			output_path: Path,
			trash_bin_path: Path,
			*,
			apply_backup_attrs_func: Callable[[schema.File, Path], None],
	):
		self.logger = logger.get()
		self.output_path = output_path
		self.trash_bin_path = trash_bin_path
		self.__apply_backup_attrs = apply_backup_attrs_func
		self.__lock = threading.Lock()
		self.__moved_files: List[_ReuseCandidate] = []
		self.__files_needing_attr_restore: Dict[Path, _FileAttrs] = {}
		self.__source_directory_times: Dict[Path, _DirectoryTimes] = {}

	@staticmethod
	def __metadata_matches(file: schema.File, st: os.stat_result) -> bool:
		if file.mode != st.st_mode:
			return False
		if file.uid != st.st_uid or file.gid != st.st_gid:
			return False
		if file.mtime is not None and file.mtime_unix_ns != st.st_mtime_ns:
			return False
		return True

	def try_reuse(self, file: schema.File, relative_path: Path) -> bool:
		if not stat.S_ISREG(file.mode):
			return False

		candidate = self.__inspect_candidate(file, relative_path)
		if candidate is None:
			return False
		try:
			return self.__move_candidate_into_place(candidate)
		except OSError as e:
			self.logger.warning(
				'Cannot move reusable restore file {!r} into place: {}; exporting from backup'.format(
					relative_path.as_posix(), e,
				)
			)
			return False

	def __inspect_candidate(self, file: schema.File, relative_path: Path) -> Optional[_ReuseCandidate]:
		trash_path = self.trash_bin_path / relative_path
		try:
			st = trash_path.lstat()
			if not stat.S_ISREG(st.st_mode):
				return None
			if file.blob_hash is None or file.blob_raw_size != st.st_size:
				return None
			if file.uid is None or file.gid is None:
				return None
			metadata_matches = self.__metadata_matches(file, st)
			if not metadata_matches and (not _i_am_root() or st.st_nlink > 1):
				return None
			if hash_utils.calc_file_hash(trash_path) != file.blob_hash:
				return None
		except OSError as e:
			if not isinstance(e, FileNotFoundError):
				self.logger.debug('Failed to inspect reuse candidate {!r}: {}'.format(relative_path.as_posix(), e))
			return None

		return _ReuseCandidate(
			file=file,
			relative_path=relative_path,
			trash_path=trash_path,
			destination_path=self.output_path / relative_path,
			original_attrs=_FileAttrs.of_stat(st),
			metadata_matches=metadata_matches,
		)

	def __move_candidate_into_place(self, candidate: _ReuseCandidate) -> bool:
		# Serialize moves and their rollback bookkeeping. This also ensures that the
		# first directory timestamp snapshot predates every rename from that directory.
		with self.__lock:
			source_directory = candidate.trash_path.parent
			directory_times = self.__source_directory_times.get(source_directory)
			if directory_times is None:
				directory_times = _DirectoryTimes.read(source_directory)
			os.replace(candidate.trash_path, candidate.destination_path)
			self.__source_directory_times.setdefault(source_directory, directory_times)
			self.__moved_files.append(candidate)
			if candidate.metadata_matches:
				return True
			return self.__repair_metadata_or_move_back(candidate)

	def __repair_metadata_or_move_back(self, candidate: _ReuseCandidate) -> bool:
		try:
			self.__apply_backup_attrs(candidate.file, candidate.destination_path)
			if not self.__metadata_matches(candidate.file, candidate.destination_path.lstat()):
				raise OSError('restored metadata does not match the backup')
		except OSError as e:
			needs_attr_restore = False
			try:
				candidate.original_attrs.apply(candidate.destination_path)
			except OSError as restore_error:
				needs_attr_restore = True
				self.logger.warning(
					'Cannot immediately restore original metadata for {!r}: {}'.format(
						candidate.relative_path.as_posix(), restore_error,
					)
				)
			try:
				os.replace(candidate.destination_path, candidate.trash_path)
			except OSError as move_back_error:
				rbe = RestoreFileReuseRollbackError('Cannot move failed reuse file {!r} back to the trash bin'.format(candidate.relative_path.as_posix()))
				raise rbe from move_back_error
			if needs_attr_restore:
				self.__files_needing_attr_restore[candidate.trash_path] = candidate.original_attrs
			self.__moved_files.pop()
			self.logger.warning(
				'Cannot apply backup metadata to reusable restore file {!r}: {}; '
				'exporting from backup'.format(candidate.relative_path.as_posix(), e)
			)
			return False
		return True

	def rollback(self):
		for moved_file in reversed(self.__moved_files):
			if os.path.lexists(moved_file.destination_path):
				os.replace(moved_file.destination_path, moved_file.trash_path)
				moved_file.original_attrs.apply(moved_file.trash_path)
		self.__moved_files.clear()

		for path, attrs in self.__files_needing_attr_restore.items():
			if os.path.lexists(path):
				attrs.apply(path)
		self.__files_needing_attr_restore.clear()

		for path, times in sorted(
				self.__source_directory_times.items(),
				key=lambda item: len(item[0].parts),
				reverse=True,
		):
			times.apply(path)
		self.__source_directory_times.clear()

	def commit(self):
		self.logger.info('Restore file reuse moved {} unchanged files, size {} bytes'.format(
			len(self.__moved_files),
			sum((moved_file.file.blob_raw_size or 0) for moved_file in self.__moved_files),
		))
		self.__moved_files.clear()
		self.__files_needing_attr_restore.clear()
		self.__source_directory_times.clear()
