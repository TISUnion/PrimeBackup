import contextlib
import logging
from pathlib import Path
from typing import ContextManager

from prime_backup import constants

LOG_FORMATTER = logging.Formatter('[%(asctime)s %(levelname)s] (%(funcName)s) %(message)s')
LOG_FORMATTER_NO_FUNC = logging.Formatter('[%(asctime)s %(levelname)s] %(message)s')


def __get_log_mode() -> int:
	from prime_backup.config.config import Config
	return logging.DEBUG if Config.get().debug else logging.INFO


def __get_log_file_path(file_name: str) -> Path:
	from prime_backup.config.config import Config
	return Config.get().storage_path / 'logs' / file_name


def create_file_logger(name: str) -> logging.Logger:
	logger = logging.Logger(f'{constants.PLUGIN_ID}-{name}', __get_log_mode())
	log_file = __get_log_file_path(f'{name}.log')
	log_file.parent.mkdir(parents=True, exist_ok=True)
	handler = logging.FileHandler(log_file, encoding='utf8')
	handler.setFormatter(LOG_FORMATTER)
	logger.addHandler(handler)
	return logger


@contextlib.contextmanager
def open_file_logger(name: str) -> ContextManager[logging.Logger]:
	logger = create_file_logger(name)
	try:
		yield logger
	finally:
		for hdr in list(logger.handlers):
			logger.removeHandler(hdr)
