from typing import NamedTuple, Union, TYPE_CHECKING

from prime_backup import constants

if TYPE_CHECKING:
	from mcdreforged.api.all import CommandSource, RTextBase


class Operator(NamedTuple):
	type: str
	name: str

	@classmethod
	def pb(cls, what: str) -> 'Operator':
		return Operator(constants.PLUGIN_ID, what)

	@classmethod
	def player(cls, name: str) -> 'Operator':
		return Operator('player', name)

	@classmethod
	def console(cls) -> 'Operator':
		return Operator('console', '')

	@classmethod
	def of(cls, value: Union[str, 'CommandSource']) -> 'Operator':
		from mcdreforged.api.all import CommandSource
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

	def to_text(self) -> 'RTextBase':
		from prime_backup.mcdr.text_components import TextComponents
		return TextComponents.operator(self)

	def __str__(self):
		return f'{self.type}:{self.name}'
