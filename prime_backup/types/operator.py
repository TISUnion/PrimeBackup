import dataclasses
from typing import Union, TYPE_CHECKING

from prime_backup import constants

if TYPE_CHECKING:
	from mcdreforged.api.all import CommandSource, RTextBase


class _PrimeBackupOperatorName(str):
	pass


class PrimeBackupOperatorNames:
	"""
	For :meth:`prime_backup.types.operator.Operator.pb`
	"""
	import_ = _PrimeBackupOperatorName('import')
	pre_restore = _PrimeBackupOperatorName('pre_restore')
	scheduled_backup = _PrimeBackupOperatorName('scheduled_backup')
	test = _PrimeBackupOperatorName('test')


@dataclasses.dataclass(frozen=True)
class Operator:
	type: str
	name: str

	@classmethod
	def unknown(cls) -> 'Operator':
		return Operator('unknown', '')

	@classmethod
	def pb(cls, pb_op_name: _PrimeBackupOperatorName) -> 'Operator':
		return Operator(constants.PLUGIN_ID, str(pb_op_name))

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

	def is_player(self) -> bool:
		return self.type == 'player'
