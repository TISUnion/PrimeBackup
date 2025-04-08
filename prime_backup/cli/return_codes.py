import enum
import sys

from typing_extensions import NoReturn


class ErrorReturnCodes(enum.Enum):
	argparse_error = 2  # see argparse.ArgumentParser.error
	action_failed = 3
	backup_not_found = 4
	backup_file_not_found = 5

	def sys_exit(self) -> NoReturn:
		sys.exit(self.value)
