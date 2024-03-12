import datetime
import stat
from pathlib import Path
from typing import Any, Union, Optional, Iterable

from mcdreforged.api.all import *

from prime_backup import constants
from prime_backup.compressors import CompressMethod
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.backup_tags import BackupTagName
from prime_backup.types.blob_info import BlobListSummary
from prime_backup.types.hash_method import HashMethod
from prime_backup.types.operator import Operator
from prime_backup.types.units import ByteCount, Duration
from prime_backup.utils import conversion_utils, misc_utils, backup_utils
from prime_backup.utils.mcdr_utils import mkcmd, click_and_run


class TextColors:
	backup_id = RColor.gold
	backup_tag = RColor.aqua
	byte_count = RColor.green
	date = RColor.aqua
	file = RColor.dark_aqua
	help_title = RColor.light_purple
	job_id = RColor.green
	number = RColor.yellow


class TextComponents:
	@classmethod
	def tr(cls, key, *args, **kwargs):
		from prime_backup.utils.mcdr_utils import tr
		return tr('text_components.' + key, *args, **kwargs)

	@classmethod
	def auto(cls, value: Any) -> RTextBase:
		if isinstance(value, bool):
			return cls.boolean(value)
		elif isinstance(value, (int, float)):
			return cls.number(value)
		elif isinstance(value, Duration):
			return cls.duration(value)
		elif isinstance(value, Operator):
			return cls.operator(value)
		elif isinstance(value, ByteCount):
			return cls.file_size(value)
		elif isinstance(value, Path):
			return cls.file_name(value)
		elif isinstance(value, datetime.datetime):
			return cls.date(value)
		else:
			return RTextBase.from_any(value)

	@classmethod
	def backup_brief(cls, backup: BackupInfo, *, backup_id_fancy: bool = True) -> RTextBase:
		# "backup #1: foobar"
		return RTextList(cls.tr(
			'backup_brief',
			cls.backup_id(backup.id, hover=backup_id_fancy, click=backup_id_fancy),
			cls.backup_comment(backup.comment),
		))

	@classmethod
	def backup_comment(cls, comment: str) -> RTextBase:
		if len(comment) > 0:
			if (er := backup_utils.extract_backup_comment_translation_key(comment)) is not None:
				args = er.args
				if er.key == 'pre_restore' and len(args) == 0:
					args = ('?',)
				return cls.tr(f'backup_comment.{er.key}', *args)
			return RText(comment)
		else:
			return cls.tr('backup_comment.none').set_color(RColor.gray).set_styles(RStyle.italic)

	@classmethod
	def backup_date(cls, backup: BackupInfo):
		return cls.date(backup.date)

	@classmethod
	def backup_full(cls, backup: BackupInfo, operation_buttons: bool = False, *, show_flags: bool = False) -> RTextBase:
		# "[#1] [>] [x] H-- 1.2GiB 2023-11-30 09:30:13: foobar"
		t_bid = cls.backup_id(backup.id)

		rtl = RTextList(RText('[', RColor.gray), t_bid, RText('] ', RColor.gray))
		if operation_buttons:
			rtl.append(RText('[>]', color=RColor.dark_green).h(cls.tr('backup_full.restore', t_bid)).c(RAction.suggest_command, mkcmd(f'back {backup.id}')), ' ')
			if not backup.tags.is_protected():
				rtl.append(RText('[x]', color=RColor.red).h(cls.tr('backup_full.delete', t_bid)).c(RAction.suggest_command, mkcmd(f'delete {backup.id}')), ' ')
			else:
				rtl.append(RText('[x]', color=RColor.dark_gray).h(cls.tr('backup_full.protected', t_bid)), ' ')

		if show_flags:
			for name in [BackupTagName.hidden, BackupTagName.temporary, BackupTagName.protected]:
				misc_utils.assert_true(name.value.type is bool, 'it should be a bool field')
				flag = backup.tags.get(name) is True
				if flag:
					rtl.append(name.value.flag)
				else:
					rtl.append(RText('-', RColor.dark_gray))
			rtl.append(' ')

		rtl.append(
			cls.backup_size(backup), ' ',
			cls.backup_date(backup), RText(': ', RColor.gray),
			cls.backup_comment(backup.comment).h(cls.tr('backup_full.creator', cls.operator(backup.creator))),
		)
		return rtl

	@classmethod
	def backup_id(cls, backup_id: Union[int, BackupInfo], *, hover: bool = True, click: bool = True, backup_data: Optional[BackupInfo] = None) -> RTextBase:
		if isinstance(backup_id, BackupInfo):
			backup_id = backup_id.id
		text = RText(f'#{backup_id}', TextColors.backup_id)
		if hover:
			hover_lines = [cls.tr('backup_id.hover_id', RText(backup_id, TextColors.backup_id))]
			if backup_data is not None:
				hover_lines.extend([
					cls.tr('backup_id.hover_date', cls.backup_date(backup_data)),
					cls.tr('backup_id.hover_comment', cls.backup_comment(backup_data.comment)),
					cls.tr('backup_id.hover_creator', cls.operator(backup_data.creator)),
				])
			if click:
				hover_lines.append(cls.tr('backup_id.hover_click_hint'))
			text.h(RTextBase.join('\n', hover_lines))
		if click:
			text.c(RAction.run_command, mkcmd(f'show {backup_id}'))
		return text

	@classmethod
	def backup_id_list(cls, backup_ids: Iterable[Any], **kwargs) -> RTextBase:
		return RTextList(
			'[',
			RTextBase.join(', ', [cls.backup_id(backup_id, **kwargs) for backup_id in backup_ids]),
			']',
		)

	@classmethod
	def backup_size(cls, backup_or_blob_list_summary: Union[BackupInfo, BlobListSummary], *, ndigits: int = 2) -> RTextBase:
		b = backup_or_blob_list_summary
		return cls.file_size(b.raw_size, ndigits=ndigits).h(cls.dual_size_hover(b.raw_size, b.stored_size))

	@classmethod
	def blob_list_summary_store_size(cls, bls: BlobListSummary) -> RTextBase:
		return cls.file_size(bls.raw_size).h(cls.dual_size_hover(bls.raw_size, bls.stored_size))

	@classmethod
	def boolean(cls, value: bool) -> RTextBase:
		return RText(str(value).lower(), RColor.green if value else RColor.red)

	@classmethod
	def command(cls, s: str, *, color: RColor = RColor.gray, suggest: bool = False, run: bool = False, raw: bool = False) -> RTextBase:
		cmd = s if raw else mkcmd(s)
		text = RText(cmd, color)
		if suggest:
			text.h(cls.tr('command.suggest', cmd)).c(RAction.suggest_command, cmd)
		elif run:
			text.h(cls.tr('command.run', cmd)).c(RAction.run_command, cmd)
		return text

	@classmethod
	def compress_method(cls, compress_method: Union[str, CompressMethod]) -> RTextBase:
		if isinstance(compress_method, CompressMethod):
			compress_method = compress_method.name
		return RText(compress_method, RColor.light_purple)

	@classmethod
	def confirm_hint(cls, what: RTextBase, time_wait_text: Any):
		return cls.tr(
			'confirm_hint.base',
			time_wait_text,
			click_and_run(
				RTextList(cls.tr('confirm_hint.confirm', what), '√').set_color(RColor.yellow),
				cls.tr('confirm_hint.confirm.hover', cls.command('confirm'), what),
				mkcmd('confirm'),
			),
			click_and_run(
				RTextList(cls.tr('confirm_hint.abort', what), '×').set_color(RColor.gold),
				cls.tr('confirm_hint.abort.hover', cls.command('abort'), what),
				mkcmd('abort'),
			),
		)

	@classmethod
	def crontab(cls, crontab_str: str) -> RTextBase:
		url = 'https://crontab.guru/#' + crontab_str.replace(' ', '_')
		return RText(crontab_str, TextColors.date).h(cls.tr('crontab.help_url', cls.url(url, click=False))).c(RAction.open_url, url)

	@classmethod
	def date_diff(cls, date: datetime.datetime) -> RTextBase:
		now = datetime.datetime.now(date.tzinfo)
		diff = (date - now).total_seconds()
		if diff >= 0:
			return cls.tr('date_diff.later', cls.duration(diff))
		else:
			return cls.tr('date_diff.ago', cls.duration(-diff))

	@classmethod
	def date(cls, date: Union[datetime.datetime, int]) -> RTextBase:
		if isinstance(date, int):
			date = conversion_utils.timestamp_to_local_date(date)
		return RText(conversion_utils.datetime_to_str(date), TextColors.date).h(cls.date_diff(date))

	@classmethod
	def dual_size_hover(cls, raw_size: int, stored_size: int, *, ndigits: int = 2) -> RTextBase:
		t_raw_size = cls.file_size(raw_size, ndigits=ndigits)
		t_stored_size = cls.file_size(stored_size, ndigits=ndigits)
		t_percent = cls.percent(stored_size, raw_size)
		return cls.tr('dual_size_hover', t_stored_size, t_percent, t_raw_size)

	@classmethod
	def duration(cls, seconds_or_duration: Union[float, Duration], *, color: Optional[RColor] = TextColors.number, ndigits: int = 2) -> RTextBase:
		# full duration text, e.g. "1 minute", "2 hours"
		if isinstance(seconds_or_duration, Duration):
			duration = seconds_or_duration
		elif isinstance(seconds_or_duration, (int, float)):
			duration = Duration(seconds_or_duration)
		else:
			raise TypeError(type(seconds_or_duration))
		value, unit = duration.auto_format()
		plural_suffix = cls.tr('duration.plural_suffix') if value != 1 else ''
		text = cls.tr('duration.text', round(value, ndigits), cls.tr('duration.' + unit, plural_suffix))
		if color is not None:
			text.set_color(color)
		return text

	@classmethod
	def file_mode(cls, mode: int) -> RTextBase:
		if stat.S_ISREG(mode):
			type_flag = '-'
			color = RColor.light_purple
		elif stat.S_ISDIR(mode):
			type_flag = 'd'
			color = RColor.blue
		elif stat.S_ISLNK(mode):
			type_flag = 'l'
			color = RColor.aqua
		else:
			type_flag = '?'
			color = RColor.gray

		permissions = ''
		for i in range(9):
			permissions += 'rwx'[i % 3] if (mode >> (8 - i)) & 1 == 1 else '-'

		return RText(type_flag + permissions, color)

	@classmethod
	def file_name(cls, file_path: Path) -> RTextBase:
		return RText(file_path.name, TextColors.file).h(file_path.as_posix())

	@classmethod
	def file_size(cls, byte_cnt: Union[int, ByteCount], *, ndigits: int = 2, always_sign: bool = False, color: RColor = TextColors.byte_count) -> RTextBase:
		if not isinstance(byte_cnt, ByteCount):
			byte_cnt = ByteCount(byte_cnt)
		return RText(byte_cnt.auto_str(ndigits=ndigits, always_sign=always_sign), color=color)

	@classmethod
	def hash_method(cls, hash_method: Union[str, HashMethod]) -> RTextBase:
		if isinstance(hash_method, HashMethod):
			hash_method = hash_method.name
		return RText(hash_method, RColor.light_purple)

	@classmethod
	def number(cls, value: Any) -> RTextBase:
		return RText(value, TextColors.number)

	@classmethod
	def number_list(cls, values: Iterable[Any]) -> RTextBase:
		return RTextList(
			'[',
			RTextBase.join(', ', [cls.number(v) for v in values]),
			']',
		)

	@classmethod
	def operator(cls, op: Operator) -> RTextBase:
		tr_key = f'operator.{op.type}'
		if op.type in ['player', 'command_source']:
			return cls.tr(tr_key, op.name)
		elif op.type in ['console', 'unknown']:
			return cls.tr(tr_key)
		elif op.type == constants.PLUGIN_ID:
			from prime_backup.mcdr import mcdr_globals
			t_name = cls.tr(tr_key + '.' + op.name)
			if not mcdr_globals.server.has_translation(misc_utils.ensure_type(getattr(t_name, 'translation_key'), str)):
				t_name = RText(op.name, styles=RStyle.italic)
			return RTextList(cls.tr(tr_key), RText('-', RColor.gray), t_name).set_color(RColor.dark_aqua)
		else:
			return RText(f'{op.type}:{op.name}')

	@classmethod
	def percent(cls, value: float, total: float) -> RTextBase:
		if total != 0:
			return RText(f'{100 * value / total:.1f}%', RColor.dark_green)
		else:
			return RText('N/A', RColor.gray)

	@classmethod
	def tag_name(cls, tag_name: BackupTagName) -> RTextBase:
		return RText(tag_name.name, TextColors.backup_tag).h(tag_name.value.text)

	@classmethod
	def title(cls, text: Any) -> RTextBase:
		return RTextList(RText('======== ', RColor.gray), text, RText(' ========', RColor.gray))

	@classmethod
	def url(cls, url: str, *, click: bool = True) -> RTextBase:
		text = RText(url, RColor.blue, RStyle.underlined)
		if click:
			text.c(RAction.open_url, url)
		return text
