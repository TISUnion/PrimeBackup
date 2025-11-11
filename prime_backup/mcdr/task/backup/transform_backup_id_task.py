from typing import List

from mcdreforged.api.all import CommandSource

from prime_backup.exceptions import OffsetBackupNotFound
from prime_backup.mcdr.task.basic_task import LightTask
from prime_backup.utils.backup_id_parser import BackupIdParser


class TransformBackupIdTask(LightTask[List[int]]):
	def __init__(self, source: CommandSource, backup_id_strings: List[str]):
		super().__init__(source)
		self.backup_id_strings = backup_id_strings

	@property
	def id(self) -> str:
		return 'backup_transform_backup_id'

	def run(self, *, allow_db_access: bool = True) -> List[int]:
		parser = BackupIdParser(allow_db_access=allow_db_access)
		backup_ids: List[int] = []
		for s in self.backup_id_strings:
			try:
				backup_id = parser.parse(s)
			except BackupIdParser.OffsetBackupNotFound as e:
				raise OffsetBackupNotFound(offset=e.offset)
			else:
				backup_ids.append(backup_id)
		return backup_ids

	def needs_db_access(self) -> bool:
		parser = BackupIdParser(allow_db_access=False)
		for s in self.backup_id_strings:
			try:
				_ = parser.parse(s)
			except BackupIdParser.DbAccessNotAllowed:
				return True
		return False
