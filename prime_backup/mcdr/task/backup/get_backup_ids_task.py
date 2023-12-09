from typing import List

from prime_backup.action.list_backup_action import ListBackupIdAction
from prime_backup.mcdr.task.basic_task import LightTask


class GetBackupIdsTask(LightTask[List[int]]):
	@property
	def id(self) -> str:
		return 'backup_get_ids'

	def run(self) -> List[int]:
		return ListBackupIdAction().run()
