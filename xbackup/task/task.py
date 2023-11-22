import abc

from mcdreforged.api.all import CommandSource, PluginServerInterface


psi = PluginServerInterface.psi()


class Task(abc.ABC):
	def __init__(self, source: CommandSource):
		self.source = source
		self.logger = psi.logger

	def run(self):
		raise NotImplementedError()
