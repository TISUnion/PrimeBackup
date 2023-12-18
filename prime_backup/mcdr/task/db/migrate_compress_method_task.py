from mcdreforged.api.all import *

from prime_backup import constants
from prime_backup.action.migrate_compress_method_action import MigrateCompressMethodAction
from prime_backup.compressors import CompressMethod
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.mcdr.text_components import TextComponents


class MigrateCompressMethodTask(HeavyTask[None]):
	def __init__(self, source: CommandSource, new_compress_method: CompressMethod):
		super().__init__(source)
		self.new_compress_method = new_compress_method

	@property
	def id(self) -> str:
		return 'db_migrate_compress_method'

	def run(self):
		try:
			self.new_compress_method.value.ensure_lib()
		except ImportError as e:
			self.logger.warning('Failed to create compressor of {} due to ImportError: {}'.format(self.new_compress_method, e))
			self.reply_tr(
				'missing_library',
				TextComponents.compress_method(self.new_compress_method),
				TextComponents.url(constants.DOCUMENTATION_URL, click=True),
			)
			return

		self.reply_tr(
			'show_whats_going_on',
			TextComponents.compress_method(self.new_compress_method),
			TextComponents.file_size(self.config.backup.compress_threshold, ndigits=0),
		)
		if not self.wait_confirm(self.tr('confirm_target')):
			return

		self.reply_tr('start', TextComponents.compress_method(self.new_compress_method))

		diff = self.run_action(MigrateCompressMethodAction(self.new_compress_method))
		self.server.save_config_simple(self.config)

		self.reply_tr(
			'done',
			TextComponents.compress_method(self.new_compress_method),
			TextComponents.file_size(diff.before),
			TextComponents.file_size(diff.after),
			TextComponents.file_size(diff.diff, color=RColor.dark_green, always_sign=True),
		)
