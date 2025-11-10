from pathlib import Path

from mcdreforged.command.command_source import CommandSource
from typing_extensions import override

from prime_backup.action.delete_backup_file_action import DeleteBackupFileAction
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.mcdr.text_components import TextComponents


class DeleteBackupFileTask(HeavyTask[None]):
	def __init__(self, source: CommandSource, backup_id: int, file_path: str):
		super().__init__(source)
		self.backup_id = backup_id
		self.file_path = Path(file_path).as_posix()

	@property
	@override
	def id(self) -> str:
		return 'db_delete_backup_file'

	@override
	def run(self) -> None:
		bls = DeleteBackupFileAction(self.backup_id, self.file_path).run()
		self.reply_tr(
			'done',
			TextComponents.backup_id(self.backup_id),
			TextComponents.file_name(self.file_path),
			TextComponents.file_size(bls.stored_size),
		)
