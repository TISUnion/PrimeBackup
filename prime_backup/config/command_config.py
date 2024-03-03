from mcdreforged.api.utils import Serializable

from prime_backup import constants
from prime_backup.types.units import Duration


class CommandPermissions(Serializable):
	root: int = 0

	abort: int = 1
	back: int = 2
	confirm: int = 1
	crontab: int = 3
	database: int = 4
	delete: int = 2
	delete_range: int = 3
	diff: int = 4
	export: int = 4
	help: int = 0
	# import: int = 4  # see the __add_import_permission() function below
	list: int = 1
	make: int = 1
	prune: int = 3
	rename: int = 2
	show: int = 1
	tag: int = 3

	def get(self, literal: str) -> int:
		if literal.startswith('_'):
			raise KeyError(literal)
		return getattr(self, literal, constants.DEFAULT_COMMAND_PERMISSION_LEVEL)

	def items(self):
		return self.serialize().items()


def __add_import_permission():
	# "import" is a python syntax keyword, we have to do some hacks
	setattr(CommandPermissions, 'import', 4)

	# insert the "import" after the "help"
	annotations = list(map(tuple, CommandPermissions.__annotations__.items()))
	for i in range(len(annotations)):
		if annotations[i][0] == 'help':
			annotations.insert(i + 1, ('import', int))
	CommandPermissions.__annotations__.clear()
	CommandPermissions.__annotations__.update(dict(annotations))


__add_import_permission()


class CommandConfig(Serializable):
	prefix: str = '!!pb'
	permission: CommandPermissions = CommandPermissions()
	confirm_time_wait: Duration = Duration('60s')
	backup_on_restore: bool = True
	restore_countdown_sec: int = 10
