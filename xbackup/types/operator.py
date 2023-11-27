from typing import NamedTuple, Union

from mcdreforged.command.command_source import CommandSource

from xbackup import constants


class Operator(NamedTuple):
	type: str
	name: str

	@classmethod
	def xbackup(cls, what: str) -> 'Operator':
		return Operator(constants.PLUGIN_ID, what)

	@classmethod
	def player(cls, name: str) -> 'Operator':
		return Operator('player', name)

	@classmethod
	def console(cls) -> 'Operator':
		return Operator('console', '')

	@classmethod
	def of(cls, value: Union[str, CommandSource]) -> 'Operator':
		if isinstance(value, CommandSource):
			if value.is_player:
				# noinspection PyUnresolvedReferences
				return cls.player(value.player)
			elif value.is_console:
				return cls.console()
			else:
				return Operator('command_source', str(value))
		elif isinstance(value, str):
			if ':' in value:
				t, n = value.split(':', 1)
				return Operator(type=t, name=n)
			else:
				return Operator(type=value, name='')
		else:
			raise TypeError(value)

	def __str__(self):
		return f'{self.type}:{self.name}'
