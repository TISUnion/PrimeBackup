from abc import ABC
from typing import Any

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.types.backup_tags import BackupTagName, BackupTags


class _OperateBackupTagActionBase(Action, ABC):
	def __init__(self, backup_id: int, tag_name: BackupTagName):
		super().__init__()
		self.backup_id = backup_id
		self.tag_name = tag_name


class SetBackupTagAction(_OperateBackupTagActionBase):
	def __init__(self, backup_id: int, tag_name: BackupTagName, value: Any):
		super().__init__(backup_id, tag_name)
		self.value = value

	def run(self) -> None:
		with DbAccess.open_session() as session:
			backup = session.get_backup(self.backup_id)

			tags = BackupTags(backup.tags)
			tags.set(self.tag_name, self.value)
			backup.tags = tags.to_dict()


class ClearBackupTagAction(_OperateBackupTagActionBase):
	def run(self) -> bool:
		"""
		:return: True tag_name existed, and got deleted; False: tag_name does not exist
		"""
		with DbAccess.open_session() as session:
			backup = session.get_backup(self.backup_id)

			tags = BackupTags(backup.tags)
			ret = tags.clear(self.tag_name)
			backup.tags = tags.to_dict()

		return ret
