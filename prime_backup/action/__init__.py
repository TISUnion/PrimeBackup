"""
Actions for all kinds of DB accesses
"""
import logging
import threading
from abc import ABC, abstractmethod
from typing import TypeVar, Generic

_T = TypeVar('_T')


class Action(Generic[_T], ABC):
	def __init__(self):
		self.is_interrupted = threading.Event()

		from prime_backup import logger
		from prime_backup.config.config import Config
		self.logger: logging.Logger = logger.get()
		self.config: Config = Config.get()

	@abstractmethod
	def run(self) -> _T:
		...

	def is_interruptable(self) -> bool:
		return False

	def interrupt(self):
		self.is_interrupted.set()
