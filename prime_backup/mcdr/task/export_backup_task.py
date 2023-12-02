import enum
from pathlib import Path
from typing import NamedTuple

from mcdreforged.api.all import *

from prime_backup.action.export_backup_action import ExportBackupActions
from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.mcdr.task import OperationTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.tar_format import TarFormat
from prime_backup.utils.timer import Timer


class _ZipFormat(NamedTuple):
	extension: str


class ExportBackupFormat(enum.Enum):
	tar = TarFormat.plain
	tar_gz = TarFormat.gzip
	tar_xz = TarFormat.lzma
	tar_zst = TarFormat.zstd
	zip = _ZipFormat('.zip')


def _sanitize_file_name(s: str, max_length: int = 64):
	bad_chars = set(r'\/<>|:"*?' + '\0')
	s = s.strip().replace(' ', '_')
	s = ''.join(c for c in s if c not in bad_chars and ord(c) > 31)
	return s[:max_length]


class ExportBackupTask(OperationTask):
	def __init__(self, source: CommandSource, backup_id: int, export_format: ExportBackupFormat):
		super().__init__(source)
		self.backup_id = backup_id
		self.export_format = export_format

	@property
	def name(self) -> str:
		return 'export'

	def run(self) -> None:
		backup = GetBackupAction(self.backup_id).run()

		def make_output(extension: str) -> Path:
			name = f'backup_{backup.id}'
			if len(backup.comment) > 0:
				name += '_' + _sanitize_file_name(backup.comment)
			name += extension
			return self.config.storage_path / 'export' / name

		efv = self.export_format.value
		if isinstance(efv, TarFormat):
			path = make_output(efv.value.extension)
			action = ExportBackupActions.to_tar(self.backup_id, path, efv)
		elif isinstance(efv, _ZipFormat):
			path = make_output(efv.extension)
			action = ExportBackupActions.to_zip(self.backup_id, path)
		else:
			raise TypeError(efv)

		if path.exists():
			self.reply(self.tr('already_exists', TextComponents.file_path(path)))
			return

		self.reply(self.tr('exporting', TextComponents.backup_id(backup.id)))
		timer = Timer()
		action.run()
		t_cost = RText(f'{round(timer.get_elapsed(), 2)}s', RColor.gold)
		self.reply(self.tr('exported', TextComponents.backup_id(backup.id), TextComponents.file_path(path), t_cost, TextComponents.file_size(path.stat().st_size)))
