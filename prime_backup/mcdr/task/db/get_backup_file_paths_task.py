from typing import List, Union

from mcdreforged.api.all import CommandSource
from typing_extensions import override

from prime_backup.action.get_file_action import GetBackupFilesAction
from prime_backup.mcdr.task.backup.transform_backup_id_task import TransformBackupIdTask
from prime_backup.mcdr.task.basic_task import LightTask


class GetBackupFilePathsTask(LightTask[List[str]]):
	def __init__(self, source: CommandSource, backup_id: Union[int, str]):
		super().__init__(source)
		self.backup_id = backup_id

	@property
	@override
	def id(self) -> str:
		return 'db_get_backup_file_paths'

	@override
	def run(self) -> List[str]:
		if isinstance(self.backup_id, int):
			backup_id = self.backup_id
		else:  # backup id raw argument
			backup_id = TransformBackupIdTask(self.source, [self.backup_id]).run()[0]
		file_dict = GetBackupFilesAction(backup_id).run()
		return list(file_dict.keys())
