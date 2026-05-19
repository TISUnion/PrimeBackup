from typing_extensions import override

from prime_backup.action.compact_packs_action import CompactAllPacksAction
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.mcdr.text_components import TextComponents


class CompactPacksTask(HeavyTask[None]):
	@property
	@override
	def id(self) -> str:
		return 'db_compact_packs'

	@override
	def run(self) -> None:
		self.reply_tr('start')
		result = self.run_action(CompactAllPacksAction(threshold=1.0))

		if result.reclaimed_pack_count == 0:
			self.reply_tr('done_clean')
		else:
			self.reply_tr(
				'done',
				TextComponents.number(result.reclaimed_pack_count),
				TextComponents.file_size(result.freed_size),
			)
