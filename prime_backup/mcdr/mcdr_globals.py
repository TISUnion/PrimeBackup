import warnings

from mcdreforged.api.all import PluginServerInterface, Metadata

__all__ = [
	'server',
	'metadata',
]

server: PluginServerInterface
metadata: Metadata


def __init():
	global server, metadata
	if PluginServerInterface.si_opt() is not None:
		server = PluginServerInterface.psi()
		metadata = server.get_self_metadata()
	else:
		import os
		warnings.warn('loading {} in an environment without MCDR running'.format(os.path.basename(__file__)), stacklevel=3)


__init()


def load():
	pass
