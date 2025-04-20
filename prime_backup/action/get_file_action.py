import stat
from pathlib import Path
from typing import List, Dict

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.exceptions import PrimeBackupError
from prime_backup.types.file_info import FileInfo
from prime_backup.utils.path_like import PathLike


class GetBackupFileAction(Action[FileInfo]):
	def __init__(self, backup_id: int, file_path: PathLike):
		super().__init__()
		self.backup_id = backup_id
		self.file_path = Path(file_path).as_posix()

	@override
	def run(self) -> FileInfo:
		with DbAccess.open_session() as session:
			backup = session.get_backup(self.backup_id)
			return FileInfo.of(session.get_file_in_backup(backup, self.file_path))


class GetBackupFilesAction(Action[Dict[str, FileInfo]]):
	def __init__(self, backup_id: int):
		super().__init__()
		self.backup_id = backup_id

	@override
	def run(self) -> Dict[str, FileInfo]:
		with DbAccess.open_session() as session:
			files = session.get_backup_files(self.backup_id)
			return {file.path: FileInfo.of(file) for file in files}


class GetFilesetFileAction(Action[FileInfo]):
	def __init__(self, fileset_id: int, file_path: PathLike):
		super().__init__()
		self.fileset_id = fileset_id
		self.file_path = Path(file_path).as_posix()

	@override
	def run(self) -> FileInfo:
		with DbAccess.open_session() as session:
			session.get_fileset(self.fileset_id)  # ensure fileset exists first
			return FileInfo.of(session.get_file_in_fileset(self.fileset_id, self.file_path))


class NotDirectoryError(PrimeBackupError):
	pass


class ListBackupDirectoryFileAction(Action[List[FileInfo]]):
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

			return [
				FileInfo.of(file)
				for file in session.list_directory_files_in_backup(backup, self.dir_path)
			]
