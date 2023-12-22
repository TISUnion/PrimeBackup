from abc import ABC
from typing import Union, Any

from mcdreforged.api.all import *


def tr(key: str, *args, **kwargs) -> RTextBase:
	from prime_backup import constants
	return ServerInterface.si().rtr(constants.PLUGIN_ID + '.' + key, *args, **kwargs)


class TranslationContext(ABC):
	def __init__(self, base_key: str):
		self.__base_key = base_key

	def tr(self, key: str, *args, **kwargs) -> RTextBase:
		k = self.__base_key
		if len(key) > 0:
			k += '.' + key
		return tr(k, *args, **kwargs)


def mkcmd(s: str) -> str:
	from prime_backup.config.config import Config
	cmd = Config.get().command.prefix
	if len(s) > 0:
		cmd += ' ' + s
	return cmd


def __make_message_prefix() -> RTextBase:
	return RTextList(RText('[PB]', RColor.dark_aqua).h('Prime Backup'), ' ')


def reply_message(source: CommandSource, msg: Union[str, RTextBase], *, with_prefix: bool = True):
	if with_prefix:
		msg = RTextList(__make_message_prefix(), msg)
	source.reply(msg)


def broadcast_message(msg: Union[str, RTextBase], *, with_prefix: bool = True):
	if with_prefix:
		msg = RTextList(__make_message_prefix(), msg)
	from prime_backup.mcdr import mcdr_globals
	mcdr_globals.server.broadcast(msg)


def click_and_run(message: Any, text: Any, command: str) -> RTextBase:
	return RTextBase.from_any(message).h(text).c(RAction.run_command, command)


def are_source_same(a: CommandSource, b: CommandSource):
	if isinstance(a, PlayerCommandSource) and isinstance(b, PlayerCommandSource):
		return a.player == b.player
	elif isinstance(a, ConsoleCommandSource) and isinstance(b, ConsoleCommandSource):
		return True
	else:
		return a == b
