from mcdreforged import RTextBase
from mcdreforged.api.all import RTextList, RColor
from typing_extensions import override

from prime_backup.action.get_db_overview_action import GetDbOverviewAction
from prime_backup.mcdr.task.basic_task import LightTask
from prime_backup.mcdr.text_components import TextComponents


class ShowDbOverviewTask(LightTask[None]):
	@property
	@override
	def id(self) -> str:
		return 'db_overview'

	@override
	def run(self) -> None:
		def make_section(key: str):
			return RTextList('[', self.tr(key), ']').set_color(RColor.light_purple)

		def make_size(size: int):
			return TextComponents.file_size(size).h(f'{size} bytes')

		def make_compression_ratio(size_stored: int, size_raw: int) -> RTextBase:
			return TextComponents.percent(size_stored, size_raw, ndigits=2).h(self.tr('compression_ratio'))

		def reply_tr_dedup_stats(tr_key: str, count_deduped: int, count_raw_total: int, size_deduped: int, size_raw_total: int):
			count_saved = count_raw_total - count_deduped
			size_saved = size_raw_total - size_deduped
			self.reply_tr(
				tr_key,
				TextComponents.percent(-count_saved, count_raw_total, ndigits=2).h(RTextList(TextComponents.number(count_saved), ' / ', TextComponents.number(count_raw_total))),
				TextComponents.percent(-size_saved, size_raw_total, ndigits=2).h(RTextList(TextComponents.file_size(size_saved), ' / ', TextComponents.file_size(size_raw_total))),
			)

		result = GetDbOverviewAction().run()
		blob_store_stored_size_sum = result.direct_blob_stored_size_sum + result.chunk_stored_size_sum
		blob_store_raw_size_sum = result.direct_blob_raw_size_sum + result.chunk_raw_size_sum

		self.reply(TextComponents.title(self.tr('title')))
		self.reply_tr('db_version', TextComponents.number(result.db_version))
		self.reply_tr('db_file_size', make_size(result.db_file_size))
		self.reply_tr('hash_method', result.hash_method)

		self.reply(make_section('section_backup'))
		self.reply_tr('backup_count', TextComponents.number(result.backup_count))
		self.reply_tr('blob_store_stored_size', make_size(blob_store_stored_size_sum), make_compression_ratio(blob_store_stored_size_sum, blob_store_raw_size_sum))
		self.reply_tr('blob_store_raw_size', make_size(blob_store_raw_size_sum))

		self.reply(make_section('section_file'))
		self.reply_tr('fileset_count', TextComponents.number(result.fileset_count))
		self.reply_tr('file_count', TextComponents.number(result.file_total_count), TextComponents.number(result.file_object_count))
		self.reply_tr('file_raw_size', make_size(result.file_raw_size_sum))

		self.reply(make_section('section_blob'))
		self.reply_tr('blob_count', TextComponents.number(result.blob_count))
		self.reply_tr('blob_stored_size', make_size(result.blob_stored_size_sum), make_compression_ratio(result.blob_stored_size_sum, result.blob_raw_size_sum))
		self.reply_tr('blob_raw_size', make_size(result.blob_raw_size_sum))
		reply_tr_dedup_stats('blob_dedup_stats', result.blob_count, result.file_object_count, result.blob_raw_size_sum, result.file_raw_size_sum)

		self.reply(make_section('section_chunk'))
		self.reply_tr('chunk_count', TextComponents.number(result.chunk_count))
		self.reply_tr('chunk_stored_size', make_size(result.chunk_stored_size_sum), make_compression_ratio(result.chunk_stored_size_sum, result.chunk_raw_size_sum))
		self.reply_tr('chunk_raw_size', make_size(result.chunk_raw_size_sum))
		reply_tr_dedup_stats('chunk_dedup_stats', result.chunk_count, result.chunk_group_chunk_binding_count, result.chunk_raw_size_sum, result.chunked_blob_raw_size_sum)
