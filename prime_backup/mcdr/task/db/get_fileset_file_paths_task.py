from typing import List

from mcdreforged.api.all import CommandSource
from typing_extensions import override

from prime_backup.action.get_file_action import GetFilesetFilesAction
from prime_backup.mcdr.task.basic_task import LightTask


class GetFilesetFilePathsTask(LightTask[List[str]]):
	def __init__(self, source: CommandSource, fileset_id: int):
		super().__init__(source)
		self.fileset_id = fileset_id

	@property
	@override
	def id(self) -> str:
		return 'db_get_fileset_file_paths'

	@override
	def run(self) -> List[str]:
		file_dict = GetFilesetFilesAction(self.fileset_id).run()
		return list(file_dict.keys())
