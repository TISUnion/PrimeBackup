import functools
import operator
from typing import Callable, Dict, List, TypeVar, Any, Optional, cast

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


class _UnixUidGidNameResolver:
	def __init__(self):
		self.__uid_names: Dict[int, Optional[str]] = {}
		self.__gid_names: Dict[int, Optional[str]] = {}

	@functools.cached_property
	def supported(self) -> bool:
		try:
			import pwd, grp
		except ImportError:
			return False
		else:
			return hasattr(pwd, 'getpwuid') and hasattr(grp, 'getgrgid')

	def __get_uid_name(self, uid: int) -> Optional[str]:
		if not self.supported:
			return None
		if uid not in self.__uid_names:
			name = None
			try:
				import pwd
				name = cast(Any, pwd).getpwuid(uid).pw_name
			except (ImportError, KeyError):
				pass
			self.__uid_names[uid] = name
		return self.__uid_names[uid]

	def __get_gid_name(self, gid: int) -> Optional[str]:
		if not self.supported:
			return None
		if gid not in self.__gid_names:
			name = None
			try:
				import grp
				name = cast(Any, grp).getgrgid(gid).gr_name
			except (ImportError, KeyError):
				pass
			self.__gid_names[gid] = name
		return self.__gid_names[gid]

	@classmethod
	def __format_id(cls, id_: Optional[int], name_getter: Callable[[int], Optional[str]], id_name: str) -> RTextBase:
		if id_ is None:
			return RText('?', RColor.gray)
		if (name := name_getter(id_)) is not None:
			return RText(name, TextColors.number).h(f'{id_name}={id_}')
		return TextComponents.number(id_)

	def uid(self, uid: Optional[int]) -> RTextBase:
		return self.__format_id(uid, self.__get_uid_name, 'uid')

	def gid(self, gid: Optional[int]) -> RTextBase:
		return self.__format_id(gid, self.__get_gid_name, 'gid')

	def pair_columns(self, file: FileInfo) -> List[RTextBase]:
		if not self.supported:
			return []
		return [self.uid(file.uid), self.gid(file.gid)]


def _get_raw_size_and_hash_from_blob(blob: BlobInfo) -> SizeAndHash:
	return SizeAndHash(blob.raw_size, blob.hash)


def _map_or_none(value: Optional[_T], maper: Callable[[_T], _R]) -> Optional[_R]:
	return maper(value) if value is not None else None


def _pretty_mode(mode: int) -> RTextBase:
	return TextComponents.file_mode(mode)


def _get_file_size(file: FileInfo) -> int:
	if file.blob is not None:
		return file.blob.raw_size
	if file.content is not None:
		return len(file.content)
	return 0


def _signed_file_size(sign: str, size: int, color: RColor) -> RTextBase:
	return RTextList(RText(sign, color), TextComponents.file_size(size, color=color))


class DiffBackupTask(LightTask[None]):
	def __init__(self, source: CommandSource, backup_id_old: int, backup_id_new: int):
		super().__init__(source)
		self.backup_id_old = backup_id_old
		self.backup_id_new = backup_id_new
		self.__uid_gid_resolver = _UnixUidGidNameResolver()

	@property
	@override
	def id(self) -> str:
		return 'backup_diff'

	def __make_uid_gid_columns(self, file: FileInfo) -> RTextBase:
		columns = self.__uid_gid_resolver.pair_columns(file)
		if len(columns) == 0:
			return RTextList()
		return RTextList(RTextBase.join(' ', columns), ' ')

	@override
	def run(self) -> None:
		result = DiffBackupAction(self.backup_id_old, self.backup_id_new, compare_status=False).run()
		
		t_bid_old = TextComponents.backup_id(self.backup_id_old)
		t_bid_new = TextComponents.backup_id(self.backup_id_new)
		t_na = RText('N/A', RColor.gray)
		if result.diff_count == 0:
			self.reply_tr('no_diff', t_bid_old, t_bid_new)
			return

		old_diff_size = sum(_get_file_size(file) for file in result.deleted) + sum(_get_file_size(file) for file, _ in result.changed)
		new_diff_size = sum(_get_file_size(file) for file in result.added) + sum(_get_file_size(file) for _, file in result.changed)
		self.reply_tr(
			'found_diff',
			TextComponents.number(result.diff_count),
			t_bid_old, t_bid_new,
			RTextBase.join(' ', [
				RText(f'+{len(result.added)}', RColor.green),
				RText(f'-{len(result.deleted)}', RColor.red),
				RText(f'*{len(result.changed)}', RColor.yellow),
				_signed_file_size('-', old_diff_size, RColor.red),
				_signed_file_size('+', new_diff_size, RColor.green),
			])
		)

		def reply_single(f: FileInfo, head: RTextBase):
			text = RTextBase.format(
				'{} {} {}{}',
				head,
				_pretty_mode(f.mode),
				self.__make_uid_gid_columns(f),
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
				make_hover(_pretty_mode(old_file.mode), _pretty_mode(new_file.mode))
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
					return RTextBase.format('uid={} gid={}', self.__uid_gid_resolver.uid(f.uid), self.__uid_gid_resolver.gid(f.gid))
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
				'{} {} {}{}: {}',
				RText('[*]', RColor.yellow),
				_pretty_mode(new_file.mode),
				self.__make_uid_gid_columns(new_file),
				RText(old_file.path, TextColors.file),
				t_change,
			))
