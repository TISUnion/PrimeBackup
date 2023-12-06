import logging
from typing import Optional

_logger: Optional[logging.Logger] = None


def init_logger():
	from prime_backup.utils import log_utils
	global _logger
	_logger = log_utils.create_file_logger('db')


def get() -> logging.Logger:
	return _logger
