from mcdreforged.api.all import CommandSource
from typing_extensions import override

from prime_backup.action.compact_packs_action import CompactAllPacksAction
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.mcdr.text_components import TextComponents


class CompactPacksTask(HeavyTask[None]):
	def __init__(self, source: CommandSource, *, threshold: float):
		super().__init__(source)
		self.threshold = threshold

	@property
	@override
	def id(self) -> str:
		return 'db_compact_packs'

	@override
	def run(self) -> None:
		self.reply_tr('start', TextComponents.percent(self.threshold, 1, ndigits=0))
		result = self.run_action(CompactAllPacksAction(threshold=self.threshold))

		if result.reclaimed_pack_count == 0:
			self.reply_tr('done_clean')
		else:
			self.reply_tr(
				'done',
				TextComponents.number(result.reclaimed_pack_count),
				TextComponents.file_size(result.freed_size),
			)
