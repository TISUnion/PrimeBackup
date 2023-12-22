from pathlib import Path

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.types.file_info import FileInfo
from prime_backup.utils.path_like import PathLike


class GetFileAction(Action[FileInfo]):
	def __init__(self, backup_id: int, file_path: PathLike):
		super().__init__()
		self.backup_id = backup_id
		self.file_path = Path(file_path).as_posix()

	def run(self) -> FileInfo:
		with DbAccess.open_session() as session:
			session.get_backup(self.backup_id)  # ensure backup exists first
			return FileInfo.of(session.get_file(self.backup_id, self.file_path))
