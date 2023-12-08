import functools
import logging
import sys

from prime_backup import constants


def __create_logger() -> logging.Logger:
	from prime_backup.utils.log_utils import LOG_FORMATTER, __get_log_mode
	logger = logging.Logger(constants.PLUGIN_ID, __get_log_mode())
	handler = logging.StreamHandler(sys.stdout)
	handler.setFormatter(LOG_FORMATTER)
	logger.addHandler(handler)
	return logger


@functools.lru_cache
def get() -> logging.Logger:
	from mcdreforged.api.all import ServerInterface
	if (psi := ServerInterface.psi_opt()) is not None:
		return psi.logger
	else:
		return __create_logger()
