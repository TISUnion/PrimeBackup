from typing import List

from typing_extensions import override

from prime_backup.action.list_fileset_action import ListFilesetIdAction
from prime_backup.mcdr.task.basic_task import LightTask


class GetFilesetIdsTask(LightTask[List[int]]):
	@property
	@override
	def id(self) -> str:
		return 'db_get_fileset_ids'

	@override
	def run(self) -> List[int]:
		return ListFilesetIdAction().run()
