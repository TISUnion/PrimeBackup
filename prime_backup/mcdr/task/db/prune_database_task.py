from typing_extensions import override

from prime_backup.action.scan_and_delete_orphan_objects_action import ScanAndDeleteOrphanObjectsAction
from prime_backup.action.scan_and_delete_unknown_blob_files import ScanAndDeleteUnknownBlobFilesAction
from prime_backup.action.shrink_base_fileset_action import ShrinkAllBaseFilesetsAction
from prime_backup.mcdr.task.basic_task import HeavyTask


class PruneDatabaseTask(HeavyTask[None]):
	@property
	@override
	def id(self) -> str:
		return 'db_prune'

	@override
	def run(self) -> None:
		self.reply_tr('start')

		doo_result = ScanAndDeleteOrphanObjectsAction().run()
		saf_result = ShrinkAllBaseFilesetsAction().run()
		ubf_result = ScanAndDeleteUnknownBlobFilesAction().run()

		if doo_result.total_orphan_count + saf_result.count + ubf_result.count == 0:
			self.reply_tr('done_clean')
		else:
			self.reply_tr('done', doo_result.total_orphan_count, saf_result.count, ubf_result.count)
