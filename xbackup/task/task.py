import abc

from xbackup import logger


class Task(abc.ABC):
	def __init__(self):
		self.logger = logger.get()

	def run(self):
		raise NotImplementedError()
