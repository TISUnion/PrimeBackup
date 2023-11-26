import enum


class TaskEvent(enum.Enum):
	plugin_unload = enum.auto()
	world_save_done = enum.auto()
	operation_confirmed = enum.auto()
	operation_cancelled = enum.auto()
	operation_aborted = enum.auto()
