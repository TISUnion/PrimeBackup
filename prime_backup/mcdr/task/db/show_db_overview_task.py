from prime_backup.action.get_db_overview_action import GetDbOverviewAction
from prime_backup.mcdr.task.basic_task import ReaderTask

from prime_backup.mcdr.text_components import TextComponents


class ShowDbOverviewTask(ReaderTask[None]):
	@property
	def id(self) -> str:
		return 'db_overview'

	def run(self) -> None:
		result = GetDbOverviewAction().run()
		self.reply(TextComponents.title(self.tr('title')))
		self.reply(self.tr('db_version', TextComponents.number(result.db_version)))
		self.reply(self.tr('hash_method', result.hash_method))
		self.reply(self.tr('backup_cnt', TextComponents.number(result.backup_cnt)))
		self.reply(self.tr('file_cnt', TextComponents.number(result.file_cnt)))
		self.reply(self.tr('file_raw_size', TextComponents.file_size(result.file_raw_size_sum).h(result.file_raw_size_sum)))
		self.reply(self.tr('blob_cnt', TextComponents.number(result.blob_cnt)))
		self.reply(self.tr('blob_stored_size', TextComponents.file_size(result.blob_stored_size_sum).h(result.blob_stored_size_sum), TextComponents.percent(result.blob_stored_size_sum, result.blob_raw_size_sum)))
		self.reply(self.tr('blob_raw_size', TextComponents.file_size(result.blob_raw_size_sum).h(result.blob_raw_size_sum)))
