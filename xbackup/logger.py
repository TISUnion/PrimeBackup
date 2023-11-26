import functools
import logging
import sys

from mcdreforged.api.all import ServerInterface


def __create_logger() -> logging.Logger:
	logger = logging.Logger('xbackup')
	logger.setLevel(logging.INFO)
	handler = logging.StreamHandler(sys.stdout)
	handler.setFormatter(logging.Formatter('[%(asctime)s %(levelname)s] (%(funcName)s) %(message)s'))
	logger.addHandler(handler)
	return logger


@functools.lru_cache
def get() -> logging.Logger:
	psi = ServerInterface.psi_opt()
	if psi is not None:
		return psi.logger
	else:
		return __create_logger()
