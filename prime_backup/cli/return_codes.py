import enum
import sys
from typing import TYPE_CHECKING

from typing_extensions import NoReturn


class ErrorReturnCodes(enum.Enum):
	invalid_argument = 1
	argparse_error = 2  # see argparse.ArgumentParser.error
	action_failed = 3
	backup_not_found = 4
	backup_file_not_found = 5
	missing_dependency = 6

	if TYPE_CHECKING:
		value: int

	def sys_exit(self) -> NoReturn:
		sys.exit(self.value)
