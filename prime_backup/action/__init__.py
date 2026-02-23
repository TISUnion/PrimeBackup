"""
Actions for all kinds of DB accesses
"""
import logging
import threading
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Optional

_T = TypeVar('_T')
_S = TypeVar('_S')


class Action(Generic[_T], ABC):
	def __init__(self):
		self.is_interrupted = threading.Event()

		from prime_backup import logger
		from prime_backup.config.config import Config
		self.logger: logging.Logger = logger.get()
		self.config: Config = Config.get()

		self.__running_action: Optional[Action] = None

	@abstractmethod
	def run(self) -> _T:
		...

	def is_interruptable(self) -> bool:
		return False

	def interrupt(self):
		self.is_interrupted.set()
		if (action := self.__running_action) is not None:
			action.interrupt()

	def run_action(self, action: 'Action[_S]', auto_interrupt: bool = True) -> _S:
		if self.__running_action is not None:
			raise RuntimeError('Cannot run action twice at the same time, current: {}, new: {}'.format(self.__running_action, action))
		self.__running_action = action
		try:
			if auto_interrupt and self.is_interrupted.is_set():
				action.interrupt()
			return action.run()
		finally:
			self.__running_action = None