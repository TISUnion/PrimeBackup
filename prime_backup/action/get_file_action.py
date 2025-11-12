import stat
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Dict, Optional

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.exceptions import PrimeBackupError
from prime_backup.types.file_info import FileInfo
from prime_backup.utils.path_like import PathLike


class _GetSingleFileActionBase(Action[FileInfo], ABC):
	def __init__(self, *, count_backups: bool, sample_backup_num: Optional[int]):
		super().__init__()
		self.count_backups = count_backups
		self.sample_backup_num = sample_backup_num

	@override
	def run(self) -> FileInfo:
		with DbAccess.open_session() as session:
			file = self._get_file(session)

			backup_samples: Optional[List[schema.Backup]]
			backup_count: int
			if self.count_backups and self.sample_backup_num is not None:
				backup_samples, backup_count = session.get_backups_containing_file_with_total(file, limit=self.sample_backup_num)
			elif self.count_backups:
				backup_samples, backup_count = None, session.get_backup_count_containing_file(file)
			elif self.sample_backup_num is not None:
				backup_samples, backup_count = session.get_backups_containing_file(file), 0

			return FileInfo.of(file, backup_count=backup_count, backup_samples=backup_samples)

	@abstractmethod
	def _get_file(self, session: DbSession) -> schema.File:
		...


class GetBackupFileAction(_GetSingleFileActionBase):
	def __init__(self, backup_id: int, file_path: PathLike, *, count_backups: bool = False, sample_backup_num: Optional[int] = None):
		super().__init__(count_backups=count_backups, sample_backup_num=sample_backup_num)
		self.backup_id = backup_id
		self.file_path = Path(file_path).as_posix()

	@override
	def _get_file(self, session: DbSession) -> schema.File:
		backup = session.get_backup(self.backup_id)
		return session.get_file_in_backup(backup, self.file_path)


class GetBackupFilesAction(Action[Dict[str, FileInfo]]):
	def __init__(self, backup_id: int):
		super().__init__()
		self.backup_id = backup_id

	@override
	def run(self) -> Dict[str, FileInfo]:
		with DbAccess.open_session() as session:
			files = session.get_backup_files(self.backup_id)
			return {file.path: FileInfo.of(file) for file in files}


class GetFilesetFileAction(_GetSingleFileActionBase):
	def __init__(self, fileset_id: int, file_path: PathLike, *, count_backups: bool = False, sample_backup_num: Optional[int] = None):
		super().__init__(count_backups=count_backups, sample_backup_num=sample_backup_num)
		self.fileset_id = fileset_id
		self.file_path = Path(file_path).as_posix()

	@override
	def _get_file(self, session: DbSession) -> schema.File:
		session.get_fileset(self.fileset_id)  # ensure fileset exists first
		return session.get_file_in_fileset(self.fileset_id, self.file_path)


class NotDirectoryError(PrimeBackupError):
	pass


class _ListBackupDirectoryFileActionBase(Action[List[FileInfo]], ABC):
	def __init__(self, backup_id: int, file_path: PathLike):
		super().__init__()
		self.backup_id = backup_id
		self.dir_path = Path(file_path).as_posix()
		if self.dir_path == '.':
			self.dir_path = ''

	@override
	def run(self) -> List[FileInfo]:
		with DbAccess.open_session() as session:
			backup = session.get_backup(self.backup_id)  # ensure backup exists

			if self.dir_path != '':
				dir_file = session.get_file_in_backup(backup, self.dir_path)
				if not stat.S_ISDIR(dir_file.mode):
					raise NotDirectoryError(dir_file.mode)

			return [FileInfo.of(file) for file in self._get_files(session, backup)]

	@abstractmethod
	def _get_files(self, session: DbSession, backup: schema.Backup) -> List[schema.File]:
		...


class ListBackupDirectoryFileAction(_ListBackupDirectoryFileActionBase):
	@override
	def _get_files(self, session: DbSession, backup: schema.Backup) -> List[schema.File]:
		return session.list_directory_files_in_backup(backup, self.dir_path)


class ListBackupDirectoryTreeFileAction(_ListBackupDirectoryFileActionBase):
	@override
	def _get_files(self, session: DbSession, backup: schema.Backup) -> List[schema.File]:
		return session.list_directory_tree_files_in_backup(backup, self.dir_path)
