from mcdreforged.api.all import *

from prime_backup import constants
from prime_backup.action.get_db_meta_action import GetDbMetaAction
from prime_backup.action.migrate_hash_method_action import MigrateHashMethodAction
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.hash_method import HashMethod


class MigrateHashMethodTask(HeavyTask[None]):
	def __init__(self, source: CommandSource, new_hash_method: HashMethod):
		super().__init__(source)
		self.new_hash_method = new_hash_method

	@property
	def id(self) -> str:
		return 'db_migrate_hash_method'

	def run(self):
		try:
			self.new_hash_method.value.create_hasher()
		except ImportError as e:
			self.logger.warning('Failed to create hasher of {} due to ImportError: {}'.format(self.new_hash_method, e))
			self.reply(self.tr(
				'missing_library',
				TextComponents.hash_method(self.new_hash_method),
				TextComponents.url(constants.DOCUMENTATION_URL, click=True),
				str(e)
			))
			return

		db_meta = self.run_action(GetDbMetaAction())
		if db_meta.hash_method == self.new_hash_method.name:
			self.reply(self.tr('hash_method_unchanged', TextComponents.hash_method(self.new_hash_method)))
			return

		self.reply(self.tr('show_whats_going_on', TextComponents.hash_method(db_meta.hash_method), TextComponents.hash_method(self.new_hash_method)))
		wr = self.wait_confirm(self.tr('confirm_target'))
		if not wr.is_set():
			self.reply(self.tr('no_confirm'))
			return
		elif wr.get().is_cancelled():
			self.reply(self.tr('aborted'))
			return

		self.run_action(MigrateHashMethodAction(self.new_hash_method))
		self.server.save_config_simple(self.config)

		self.reply(self.tr('done', TextComponents.hash_method(db_meta.hash_method), TextComponents.hash_method(self.new_hash_method)))
