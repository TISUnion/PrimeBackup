from pathlib import Path

from mcdreforged.api.all import *

from prime_backup.action.export_backup_action import ExportBackupToZipAction, ExportBackupToTarAction
from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.standalone_backup_format import ZipFormat, StandaloneBackupFormat
from prime_backup.types.tar_format import TarFormat
from prime_backup.utils.timer import Timer


def _sanitize_file_name(s: str, max_length: int = 64):
	bad_chars = set(r'\/<>|:"*?' + '\0')
	s = s.strip().replace(' ', '_')
	s = ''.join(c for c in s if c not in bad_chars and ord(c) > 31)
	return s[:max_length]


class ExportBackupTask(HeavyTask[None]):
	def __init__(
			self, source: CommandSource, backup_id: int, export_format: StandaloneBackupFormat, *,
			fail_soft: bool, verify_blob: bool, overwrite_existing: bool, create_meta: bool,
	):
		super().__init__(source)
		self.backup_id = backup_id
		self.export_format = export_format
		self.fail_soft = fail_soft
		self.verify_blob = verify_blob
		self.overwrite_existing = overwrite_existing
		self.create_meta = create_meta

	@property
	def id(self) -> str:
		return 'backup_export'

	def run(self) -> None:
		backup = GetBackupAction(self.backup_id).run()

		def make_output(extension: str) -> Path:
			name = f'backup_{backup.id}'
			if len(backup.comment) > 0:
				comment = TextComponents.backup_comment(backup.comment).to_plain_text()  # use MCDR's language
				name += '_' + _sanitize_file_name(comment)
			name += extension
			return self.config.storage_path / 'export' / name

		efv = self.export_format.value
		kwargs = dict(
			fail_soft=self.fail_soft,
			verify_blob=self.verify_blob,
			create_meta=self.create_meta,
		)
		if isinstance(efv, TarFormat):
			path = make_output(efv.value.extension)
			action = ExportBackupToTarAction(self.backup_id, path, efv, **kwargs)
		elif isinstance(efv, ZipFormat):
			path = make_output(efv.extension)
			action = ExportBackupToZipAction(self.backup_id, path, **kwargs)
		else:
			raise TypeError(efv)

		if path.exists() and not self.overwrite_existing:
			self.reply_tr('already_exists', TextComponents.file_name(path))
			return

		self.reply_tr('exporting', TextComponents.backup_id(backup.id))
		timer = Timer()
		failures = self.run_action(action)
		t_cost = RText(f'{round(timer.get_elapsed(), 2)}s', RColor.gold)

		if path.is_file():
			self.reply_tr('exported', TextComponents.backup_id(backup.id), TextComponents.file_name(path), t_cost, TextComponents.file_size(path.stat().st_size))
		else:
			self.reply_tr('unfinished')
		if len(failures) > 0:
			self.reply_tr('failures', len(failures))
			for line in failures.to_lines():
				self.reply(line)
