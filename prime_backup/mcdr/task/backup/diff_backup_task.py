import operator
from typing import Callable, List, TypeVar, Any, Optional

from mcdreforged.api.all import CommandSource, RTextBase, RText, RTextList, RColor, PermissionLevel
from typing_extensions import override

from prime_backup.action.diff_backup_action import DiffBackupAction
from prime_backup.mcdr.task.basic_task import LightTask
from prime_backup.mcdr.text_components import TextComponents, TextColors
from prime_backup.types.blob_info import BlobInfo
from prime_backup.types.file_info import FileInfo
from prime_backup.utils.hash_utils import SizeAndHash

_T = TypeVar('_T')
_R = TypeVar('_R')


def _get_raw_size_and_hash_from_blob(blob: BlobInfo) -> SizeAndHash:
	return SizeAndHash(blob.raw_size, blob.hash)


def _map_or_none(value: Optional[_T], maper: Callable[[_T], _R]) -> Optional[_R]:
	return maper(value) if value is not None else None


class DiffBackupTask(LightTask[None]):
	def __init__(self, source: CommandSource, backup_id_old: int, backup_id_new: int):
		super().__init__(source)
		self.backup_id_old = backup_id_old
		self.backup_id_new = backup_id_new

	@property
	@override
	def id(self) -> str:
		return 'backup_diff'

	@override
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
				pretty_mode(f.mode),
				TextComponents.file_path(f.path),
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

		for old_file, new_file in sorted(result.changed, key=operator.itemgetter(0)):
			hover_lines: List[RTextBase] = []

			def make_hover(what_old: Optional[_T], what_new: Optional[_T], what_mapper: Callable[[_T], Any] = lambda x: x):
				nonlocal hover_lines
				hover_lines = [
					RTextBase.format('{}: {}', t_bid_old, what_mapper(what_old) if what_old is not None else t_na),
					RTextBase.format('{}: {}', t_bid_new, what_mapper(what_new) if what_new is not None else t_na),
				]

			if old_file.mode != new_file.mode:
				t_change = self.tr('diff.mode')
				make_hover(pretty_mode(old_file.mode), pretty_mode(new_file.mode))
			elif (sah1 := _map_or_none(old_file.blob, _get_raw_size_and_hash_from_blob)) != (sah2 := _map_or_none(new_file.blob, _get_raw_size_and_hash_from_blob)):
				t_change = self.tr('diff.blob')
				n = 8
				if sah1 is not None and sah2 is not None:
					while n < min(len(sah1.hash), len(sah2.hash)):
						if sah1.hash[:n] != sah2.hash[:n]:
							break
						n += 8
				def blob_what_mapper(sah: SizeAndHash):
					return RTextList(TextComponents.blob_hash(sah.hash[:n]), ' ', TextComponents.file_size(sah.size))
				make_hover(sah1, sah2, blob_what_mapper)
			elif old_file.content != new_file.content:
				# currently only symlink uses the content
				t_change = self.tr('diff.link_target')
				make_hover(old_file.content, new_file.content, lambda t: t.decode('utf8'))
			elif old_file.uid != new_file.uid or old_file.gid != new_file.gid:
				def format_owner(f: FileInfo):
					return RTextBase.format('uid={} gid={}', TextComponents.number(f.uid), TextComponents.number(f.gid))
				t_change = self.tr('diff.owner')
				make_hover(format_owner(old_file), format_owner(new_file))
			elif old_file.mtime != new_file.mtime:
				t_change = self.tr('diff.mtime')
				make_hover(old_file.mtime, new_file.mtime, TextComponents.date_local)
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

