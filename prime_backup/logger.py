import functools
import logging
import sys

from prime_backup import constants


def __create_logger() -> logging.Logger:
	from prime_backup.utils.log_utils import LOG_FORMATTER
	logger = logging.Logger(constants.PLUGIN_ID)
	handler = logging.StreamHandler(sys.stdout)
	handler.setFormatter(LOG_FORMATTER)
	logger.addHandler(handler)
	return logger


@functools.lru_cache
def get() -> logging.Logger:
	from prime_backup.utils.log_utils import get_log_level
	from mcdreforged.api.all import ServerInterface
	if (psi := ServerInterface.psi_opt()) is not None:
		logger = psi.logger
	else:
		logger = __create_logger()
	logger.setLevel(get_log_level())
	return logger
