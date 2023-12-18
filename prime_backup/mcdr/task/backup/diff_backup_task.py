from typing import Callable, List

from mcdreforged.api.all import *

from prime_backup.action.diff_backup_action import DiffBackupAction
from prime_backup.mcdr.task.basic_task import LightTask
from prime_backup.mcdr.text_components import TextComponents, TextColors
from prime_backup.types.file_info import FileInfo
from prime_backup.utils import conversion_utils


class DiffBackupTask(LightTask[None]):
	def __init__(self, source: CommandSource, backup_id_old: int, backup_id_new: int):
		super().__init__(source)
		self.backup_id_old = backup_id_old
		self.backup_id_new = backup_id_new

	@property
	def id(self) -> str:
		return 'backup_diff'

	def run(self) -> None:
		result = DiffBackupAction(self.backup_id_old, self.backup_id_new, compare_status=False).run()
		
		t_bid_old = TextComponents.backup_id(self.backup_id_old)
		t_bid_new = TextComponents.backup_id(self.backup_id_new)
		t_na = RText('N/A', RColor.gray)
		if result.diff_count == 0:
			self.reply_tr('no_diff', t_bid_old, t_bid_new)
			return

		self.reply_tr(
			'found_diff',
			TextComponents.number(result.diff_count),
			t_bid_old, t_bid_new,
			RTextBase.join(' ', [
				RText(f'+{len(result.added)}', RColor.green),
				RText(f'-{len(result.deleted)}', RColor.red),
				RText(f'*{len(result.changed)}', RColor.yellow),
			])
		)

		def pretty_mode(mode: int) -> RTextBase:
			return TextComponents.file_mode(mode)

		def reply_single(f: FileInfo, head: RTextBase):
			text = RTextBase.format(
				'{} {} {}',
				head,
				pretty_mode(file.mode),
				RText(file.path, TextColors.file),
			)
			if f.is_link():
				if f.content is not None:
					target = RText(f.content.decode('utf8'), TextColors.file)
				else:
					target = RText('?', RColor.gray)
				text = RTextList(text, ' -> ', target)
			self.reply(text)

		for file in sorted(result.added):
			reply_single(file, RText('[+]', RColor.green))
		for file in sorted(result.deleted):
			reply_single(file, RText('[-]', RColor.red))

		for old_file, new_file in sorted(result.changed, key=lambda f: f[0]):
			def make_hover(what_old, what_new, what_mapper: Callable = lambda x: x):
				nonlocal hover_lines
				hover_lines = [
					RTextBase.format('{}: {}', t_bid_old, what_mapper(what_old) if what_old is not None else t_na),
					RTextBase.format('{}: {}', t_bid_new, what_mapper(what_new) if what_new is not None else t_na),
				]

			def map_or_none(value, maper: Callable):
				return maper(value) if value is not None else None

			hover_lines: List[RTextBase] = []
			if old_file.mode != new_file.mode:
				t_change = self.tr('diff.mode')
				make_hover(pretty_mode(old_file.mode), pretty_mode(new_file.mode))
			elif (h1 := map_or_none(old_file.blob, lambda b: b.hash)) != (h2 := map_or_none(new_file.blob, lambda b: b.hash)):
				t_change = self.tr('diff.blob')
				n = 8
				if h1 is not None and h2 is not None:
					while n < min(len(h1), len(h2)):
						if h1[:n] != h2[:n]:
							break
						n += 8
				make_hover(h1, h2, lambda h: h[:n])
			elif old_file.content != new_file.content:
				# currently only symlink uses the content
				t_change = self.tr('diff.link_target')
				make_hover(old_file.content, new_file.content, lambda t: t.decode('utf8'))
			elif old_file.uid != new_file.uid or old_file.gid != new_file.gid:
				def format_owner(f: FileInfo):
					return RTextBase.format('uid={} gid={}', TextComponents.number(f.uid), TextComponents.number(f.gid))
				t_change = self.tr('diff.owner')
				make_hover(format_owner(old_file), format_owner(new_file))
			elif old_file.mtime_ns != new_file.mtime_ns:
				t_change = self.tr('diff.mtime')
				make_hover(old_file.mtime_ns, new_file.mtime_ns, lambda mt: TextComponents.date(conversion_utils.timestamp_to_local_date(mt)))
			else:
				t_change = self.tr('diff.other').set_color(RColor.gray)

			if len(hover_lines) > 0 and self.source.has_permission(PermissionLevel.PHYSICAL_SERVER_CONTROL_LEVEL):
				if self.source.is_player:
					t_change.h(RTextBase.join('\n', hover_lines))
				else:
					t_change = RTextList(t_change, ' (', RTextBase.join(', ', hover_lines), ')')

			self.reply(RTextBase.format(
				'{} {} {}: {}',
				RText('[*]', RColor.yellow),
				pretty_mode(new_file.mode),
				RText(old_file.path, TextColors.file),
				t_change,
			))

