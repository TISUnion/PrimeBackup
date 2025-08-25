from pathlib import Path

from mcdreforged.api.all import PluginServerInterface, SimpleCommandBuilder, CommandSource, RColor

from prime_backup import logger
from prime_backup.config.config import Config
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.utils import mcdr_utils


class DisabledCommandHelper:
	def __init__(self, server: PluginServerInterface, self_name: str):
		self.config = Config.get()
		self.logger = logger.get()

		self.server = server
		self.self_name = self_name
		self.__mark_file = Path(server.get_data_folder()) / '.enabled_mark'

	def on_disabled(self):
		if self.__mark_file.exists():
			return
		self.__register_disabled_command()

	def __register_disabled_command(self):
		builder = SimpleCommandBuilder()

		@builder.command(self.config.command.prefix)
		def handle_root(source: CommandSource):
			with source.preferred_language_context():
				doc_url = mcdr_utils.tr('command.disabled.doc_url').to_plain_text()
			mcdr_utils.reply_message(source, mcdr_utils.tr('command.disabled.disabled_by_config', self.self_name).set_color(RColor.yellow))
			mcdr_utils.reply_message(source, mcdr_utils.tr('command.disabled.read_doc', self.self_name, TextComponents.url(doc_url)))

		builder.register(self.server)

	def on_enabled(self):
		try:
			if not self.__mark_file.exists():
				self.__mark_file.write_bytes(b'')
		except Exception as e:
			self.logger.error('Failed to create enabled mark file at {}: {}'.format(self.__mark_file, e))
