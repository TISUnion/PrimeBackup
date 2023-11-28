from abc import ABC
from typing import Union, Any

from mcdreforged.api.all import *

from prime_backup import constants
from prime_backup.config.config import Config
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.operator import Operator


def tr(key: str, *args, **kwargs) -> RTextBase:
	return ServerInterface.si().rtr(constants.PLUGIN_ID + '.' + key, *args, **kwargs)


class TranslationContext(ABC):
	def __init__(self, base_key: str):
		self.__base_key = base_key

	def tr(self, key: str, *args, **kwargs) -> RTextBase:
		k = self.__base_key
		if len(key) > 0:
			k += '.' + key
		return tr(k, *args, **kwargs)


class Texts:
	@classmethod
	def title(cls, text: Any):
		return RTextList(RText('======== ', RColor.gray), text, RText(' ========', RColor.gray))

	@classmethod
	def file_size(cls, byte_cnt: int, *, ndigits: int = 2) -> RTextBase:
		from prime_backup.config.types import ByteCount
		number, unit = ByteCount(byte_cnt).auto_format()
		return RText(f'{round(number, ndigits)}{unit}', color=RColor.green)

	@classmethod
	def backup(cls, backup: BackupInfo, **kwargs) -> RTextBase:
		return RTextList(tr(
			'texts.backup',
			cls.backup_id(backup.id, **kwargs),
			cls.backup_comment(backup.comment),
		))

	@classmethod
	def backup_id(cls, backup_id: int, *, hover: bool = True, click: bool = True) -> RTextBase:
		text = RText(f'#{backup_id}', RColor.gold)
		if hover:
			text.h(tr('texts.backup_id.hover', backup_id))
		if click:
			text.c(RAction.run_command, mkcmd(f'show {backup_id}'))
		return text

	@classmethod
	def backup_comment(cls, comment: str) -> RTextBase:
		return RText(comment) if len(comment) > 0 else tr('texts.backup_comment.none').set_color(RColor.gray)

	@classmethod
	def command(cls, s: str, *, color: RColor = RColor.gray, suggest: bool = False, run: bool = False, raw: bool = False) -> RTextBase:
		cmd = s if raw else mkcmd(s)
		text = RText(cmd, color)
		if suggest:
			text.h(tr('texts.command.suggest', cmd)).c(RAction.suggest_command, cmd)
		elif run:
			text.h(tr('texts.command.run', cmd)).c(RAction.run_command, cmd)
		return text

	@classmethod
	def operator(cls, op: Operator) -> RTextBase:
		if op.type == 'player':
			return tr('texts.operator.player', op.name)
		elif op.type == 'console':
			return tr('texts.operator.console')
		elif op.type == 'command_source':
			return tr('texts.operator.command_source', op.name)
		else:
			return tr('texts.operator.other', op.name, op.type)


def mkcmd(s: str) -> str:
	cmd = Config.get().command.prefix
	if len(s) > 0:
		cmd += ' ' + s
	return cmd


def __make_message_prefix() -> RTextBase:
	return RTextList(RText('[PB]', RColor.aqua).h('Prime Backup'), ' ')


def reply_message(source: CommandSource, msg: Union[str, RTextBase], *, with_prefix: bool = True):
	if with_prefix:
		msg = RTextList(__make_message_prefix(), msg)
	source.reply(msg)


def broadcast_message(source: CommandSource, msg: Union[str, RTextBase], *, with_prefix: bool = True):
	if with_prefix:
		msg = RTextList(__make_message_prefix(), msg)
	source.get_server().say(msg)


def click_and_run(message: Any, text: Any, command: str) -> RTextBase:
	return RTextBase.from_any(message).h(text).c(RAction.run_command, command)
