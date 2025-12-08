import contextlib
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Generator

LOG_FORMATTER = logging.Formatter('[%(asctime)s %(levelname)s] (%(funcName)s) %(message)s')
LOG_FORMATTER_NO_FUNC = logging.Formatter('[%(asctime)s %(levelname)s] %(message)s')
LOG_FORMATTER.default_msec_format = '%s.%03d'
LOG_FORMATTER_NO_FUNC.default_msec_format = '%s.%03d'


class FileLogger(logging.Logger):
	def __init__(self, name: str):
		from prime_backup import constants
		super().__init__(f'{constants.PLUGIN_ID}-{name}', get_log_level())
		self.log_file = self.__get_log_file_path(f'{name}.log')
		self.log_file.parent.mkdir(parents=True, exist_ok=True)
		handler = RotatingFileHandler(
			self.log_file,
			maxBytes=10 * 1024 * 1024,
			backupCount=1,
			encoding='utf8'
		)
		handler.setFormatter(LOG_FORMATTER)
		self.addHandler(handler)

	@classmethod
	def __get_log_file_path(cls, file_name: str) -> Path:
		from prime_backup.config.config import Config
		return Config.get().storage_path / 'logs' / file_name


def get_log_level() -> int:
	from prime_backup.config.config import Config
	return logging.DEBUG if Config.get().debug else logging.INFO


def create_file_logger(name: str) -> FileLogger:
	return FileLogger(name)


@contextlib.contextmanager
def open_file_logger(name: str) -> Generator[FileLogger, None, None]:
	logger = create_file_logger(name)
	try:
		yield logger
	finally:
		for hdr in list(logger.handlers):
			logger.removeHandler(hdr)
