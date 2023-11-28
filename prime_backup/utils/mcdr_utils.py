from abc import ABC
from typing import Union, Any

from mcdreforged.api.all import *

from prime_backup import constants


def tr(key: str, *args, **kwargs) -> RTextBase:
	return ServerInterface.si().rtr(constants.PLUGIN_ID + '.' + key, *args, **kwargs)


class TranslationContext(ABC):
	def __init__(self, base_key: str):
		self.__base_key = base_key

	def tr(self, key: str, *args, **kwargs) -> RTextBase:
		k = self.__base_key
		if len(key) > 0:
			k += '.' + self.__base_key
		return tr(k, *args, **kwargs)


class Elements:
	@classmethod
	def file_size(cls, byte_cnt: int, *, ndigits: int = 2) -> RTextBase:
		from prime_backup.config.types import ByteCount
		number, unit = ByteCount(byte_cnt).auto_format()
		return RText(f'{round(number, ndigits)}{unit}', color=RColor.yellow)

	@classmethod
	def backup_id(cls, backup_id: int) -> RTextBase:
		return RTextList(
			RText('#', RColor.gray),
			RText(backup_id, RColor.gold).h(tr('element.backup_id', backup_id)).c(RAction.suggest_command, mkcmd(f'inspect {backup_id}')),
		)


def mkcmd(s: str) -> str:
	from prime_backup.config.config import Config
	cmd = Config.get().command.prefix
	if len(s) > 0:
		cmd += ' ' + s
	return cmd


def __make_message_prefix() -> RTextBase:
	return RTextList('[', RText('PB', RColor.aqua).h('Prime Backup'), '] ')


def reply_message(source: CommandSource, msg: Union[str, RTextBase], *, with_prefix: bool = True):
	if with_prefix:
		msg = RTextList(__make_message_prefix(), msg)
	source.reply(msg)


def broadcast_message(source: CommandSource, msg: Union[str, RTextBase], *, with_prefix: bool = True):
	if with_prefix:
		msg = RTextList(__make_message_prefix(), msg)
	source.get_server().say(msg)


def click_and_run(message: Any, text: Any, command: str) -> RTextBase:
	fancy_text = message.copy() if isinstance(message, RTextBase) else RText(message)
	return fancy_text.set_hover_text(text).set_click_event(RAction.run_command, command)
