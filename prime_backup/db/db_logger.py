import logging
from pathlib import Path
from typing import Optional

from prime_backup import constants

_logger: Optional[logging.Logger] = None


def init_logger(db_dir: Path):
	from prime_backup.config.config import Config
	global _logger
	# TODO: concurrency write proof
	logs_dir = db_dir / 'logs'
	logs_dir.mkdir(parents=True, exist_ok=True)
	_logger = logging.Logger(constants.PLUGIN_ID + '_db')
	_logger.setLevel(logging.DEBUG if Config.get().debug else logging.INFO)
	handler = logging.FileHandler(logs_dir / 'db.log', encoding='utf8')
	handler.setFormatter(logging.Formatter('[%(asctime)s %(levelname)s] [%(funcName)s] %(message)s'))
	_logger.addHandler(handler)


def get() -> logging.Logger:
	return _logger
