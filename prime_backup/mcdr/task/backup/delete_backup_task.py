from typing import List

from mcdreforged.api.all import *
from typing_extensions import override

from prime_backup.action.delete_backup_action import DeleteBackupAction
from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.blob_info import BlobListSummary
from prime_backup.utils import collection_utils


class DeleteBackupTask(HeavyTask[None]):
	def __init__(self, source: CommandSource, backup_ids: List[int]):
		super().__init__(source)
		self.backup_ids = collection_utils.deduplicated_list(backup_ids)
		if len(self.backup_ids) == 0:
			raise ValueError()

	@property
	@override
	def id(self) -> str:
		return 'backup_delete'

	@override
	def run(self):
		if len(self.backup_ids) > 1:
			self.reply_tr(
				'multi_delete',
				TextComponents.number(len(self.backup_ids)),
				TextComponents.backup_id_list(self.backup_ids, hover=False, click=False),
			)

		bls_total = BlobListSummary.zero()
		for backup_id in self.backup_ids:
			backup = GetBackupAction(backup_id).run()
			if backup.tags.is_protected():
				self.reply_tr('protected', TextComponents.backup_id(backup.id))
				return

			self.reply_tr('deleting', TextComponents.backup_brief(backup, backup_id_fancy=False))
			dr = DeleteBackupAction(backup_id).run()
			bls_total += dr.bls

			self.reply_tr(
				'deleted',
				TextComponents.backup_id(backup_id, hover=False, click=False),
				TextComponents.blob_list_summary_store_size(dr.bls),
			)

		if len(self.backup_ids) > 1:
			self.reply_tr(
				'multi_deleted',
				TextComponents.number(len(self.backup_ids)),
				TextComponents.blob_list_summary_store_size(bls_total),
			)