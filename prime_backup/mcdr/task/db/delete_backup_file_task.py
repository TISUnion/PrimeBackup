import collections
from pathlib import Path

from mcdreforged.api.all import CommandSource, RColor
from typing_extensions import override

from prime_backup.action.delete_backup_file_action import DeleteBackupFileAction, DeleteDirectoryNotAllowed
from prime_backup.action.get_file_action import GetBackupFileAction, ListBackupDirectoryTreeFileAction
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.file_info import FileType


class DeleteBackupFileTask(HeavyTask[None]):
	def __init__(self, source: CommandSource, backup_id: int, file_path: str, *, needs_confirm: bool = True, recursive: bool = False):
		super().__init__(source)
		self.backup_id = backup_id
		self.file_path = Path(file_path).as_posix()
		self.needs_confirm = needs_confirm
		self.recursive = recursive

	@property
	@override
	def id(self) -> str:
		return 'db_delete_backup_file'

	def __reply_cannot_delete_dir(self):
		self.reply(self.tr('cannot_delete_directory', self.file_path).set_color(RColor.red))

	@override
	def run(self) -> None:
		if self.file_path in ['.', '']:
			self.reply(self.tr('cannot_delete_root').set_color(RColor.red))
			return

		self.reply_tr('start', TextComponents.backup_id(self.backup_id), TextComponents.file_name(self.file_path))

		file = GetBackupFileAction(self.backup_id, self.file_path).run()
		if file.is_dir():
			if not self.recursive:
				self.__reply_cannot_delete_dir()
				return
			all_files = ListBackupDirectoryTreeFileAction(self.backup_id, self.file_path).run()  # XXX: handle NotDirectoryError?
			raw_size_sum = sum([(file.blob.raw_size if file.blob is not None else 0) for file in all_files])
			file_type_counts = collections.Counter(file.file_type for file in all_files)
			self.reply_tr(
				'info.directory', TextComponents.file_type(file.file_type), len(all_files),
				file_type_counts.get(FileType.file, 0), file_type_counts.get(FileType.directory, 0),
				TextComponents.file_size(raw_size_sum),
			)
		else:
			self.reply_tr('info.regular', TextComponents.file_type(file.file_type), TextComponents.file_size(file.blob.raw_size) if file.blob is not None else 'N/A')

		if self.needs_confirm and not self.wait_confirm(self.tr('confirm_target')):
			return

		try:
			bls = DeleteBackupFileAction(self.backup_id, self.file_path, self.recursive).run()
		except DeleteDirectoryNotAllowed:
			self.__reply_cannot_delete_dir()
			return

		self.reply_tr('done', TextComponents.backup_id(self.backup_id), TextComponents.file_name(self.file_path), TextComponents.file_size(bls.stored_size))
