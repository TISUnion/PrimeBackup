import dataclasses
from typing import List, Dict, Tuple

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.types.file_info import FileInfo


@dataclasses.dataclass(frozen=True)
class DiffResult:
	added: List[FileInfo] = dataclasses.field(default_factory=list)
	deleted: List[FileInfo] = dataclasses.field(default_factory=list)
	changed: List[Tuple[FileInfo, FileInfo]] = dataclasses.field(default_factory=list)  # (old, new)

	@property
	def diff_count(self) -> int:
		return len(self.added) + len(self.changed) + len(self.deleted)


class DiffBackupAction(Action[DiffResult]):
	def __init__(self, backup_id_old: int, backup_id_new: int, *, compare_status: bool):
		super().__init__()
		self.backup_id_old = backup_id_old
		self.backup_id_new = backup_id_new
		self.compare_status = compare_status

	@classmethod
	def __get_files_from_backup(cls, session: DbSession, backup_id: int) -> Dict[str, FileInfo]:
		files = {}
		for file in session.get_backup(backup_id).files:
			files[file.path] = FileInfo.of(file)
		return files

	def __compare_files(self, a: FileInfo, b: FileInfo) -> bool:
		return (
				True
				and a.path == b.path
				and a.mode == b.mode
				and getattr(a.blob, 'hash', None) == getattr(b.blob, 'hash', None)
				and a.content == b.content
				and (not self.compare_status or (
						True
						and a.uid == b.uid
						and a.gid == b.gid
						and a.mtime_ns == b.mtime_ns
				))
		)

	def run(self) -> DiffResult:
		with DbAccess.open_session() as session:
			files_old = self.__get_files_from_backup(session, self.backup_id_old)
			files_new = self.__get_files_from_backup(session, self.backup_id_new)

		result = DiffResult()
		for path, file in files_old.items():
			if path in files_new:
				new_file = files_new[path]
				if not self.__compare_files(file, new_file):
					result.changed.append((file, new_file))
			else:
				result.deleted.append(file)
		for path, file in files_new.items():
			if path not in files_old:
				result.added.append(file)
		return result
