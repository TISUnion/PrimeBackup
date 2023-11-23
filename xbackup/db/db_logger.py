import logging
from pathlib import Path
from typing import Optional

_logger: Optional[logging.Logger] = None


def init_logger(db_dir: Path):
	global _logger
	# TODO: concurrency write proof
	logs_dir = db_dir / 'logs'
	logs_dir.mkdir(parents=True, exist_ok=True)
	_logger = logging.Logger('xbackup-db')
	_logger.setLevel(logging.DEBUG)  # TODO: configure-able
	handler = logging.FileHandler(logs_dir / 'db.log', encoding='utf8')
	handler.setFormatter(logging.Formatter('[%(asctime)s %(levelname)s] [%(funcName)s] %(message)s'))
	_logger.addHandler(handler)


def get_logger() -> logging.Logger:
	return _logger
