from typing import Union, Any

from mcdreforged.api.all import *

from prime_backup import constants


def tr(key: str, *args, **kwargs) -> RTextBase:
	return ServerInterface.si().rtr(constants.PLUGIN_ID + '.' + key, *args, **kwargs)


def mkcmd(s: str) -> str:
	from prime_backup.config.config import Config
	cmd = Config.get().command.prefix
	if len(s) > 0:
		cmd += ' ' + s
	return cmd


def print_message(source: CommandSource, msg: Union[str, RTextBase], tell: bool = True, prefix: str = '[PB] '):
	msg = RTextList(prefix, msg)
	if source.is_player and not tell:
		source.get_server().say(msg)
	else:
		source.reply(msg)


def command_run(message: Any, text: Any, command: str) -> RTextBase:
	fancy_text = message.copy() if isinstance(message, RTextBase) else RText(message)
	return fancy_text.set_hover_text(text).set_click_event(RAction.run_command, command)
