from pathlib import Path
from typing import Optional

from mcdreforged.api.all import *

from prime_backup.action.import_backup_action import ImportBackupAction, BackupMetadataNotFound
from prime_backup.mcdr import mcdr_globals
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.mcdr.text_components import TextComponents, TextColors
from prime_backup.types.standalone_backup_format import StandaloneBackupFormat


class ImportBackupTask(HeavyTask[None]):
	def __init__(self, source: CommandSource, file_path: Path, backup_format: Optional[StandaloneBackupFormat] = None, *, ensure_meta: bool):
		super().__init__(source)
		self.file_path = file_path
		self.backup_format = backup_format
		self.ensure_meta = ensure_meta

	@property
	def id(self) -> str:
		return 'backup_import'

	def run(self) -> None:
		t_fp = TextComponents.file_name(self.file_path)
		if not self.file_path.exists():
			self.reply(self.tr('file_not_found', t_fp))
			return
		if not self.file_path.is_file():
			self.reply(self.tr('not_a_file', t_fp))
			return

		if self.backup_format is None:
			if (backup_format := StandaloneBackupFormat.from_file_name(self.file_path)) is None:
				self.reply(self.tr('cannot_infer_backup_format', RText(self.file_path.name, TextColors.file)))
				return
		else:
			backup_format = self.backup_format

		self.reply(self.tr('start', t_fp, RText(backup_format.name, RColor.dark_aqua)))
		try:
			backup = self.run_action(ImportBackupAction(self.file_path, backup_format, ensure_meta=self.ensure_meta))
		except BackupMetadataNotFound as e:
			self.reply(self.tr('backup_metadata_not_found', t_fp, str(e)).set_color(RColor.red))
			self.reply(self.tr('backup_metadata_not_found.suggestion', name=mcdr_globals.metadata.name))
		else:
			self.reply(self.tr('done', t_fp, TextComponents.backup_id(backup)))