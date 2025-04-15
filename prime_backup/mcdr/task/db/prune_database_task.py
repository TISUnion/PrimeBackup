from typing_extensions import override

from prime_backup.action.list_fileset_action import ListFilesetAction
from prime_backup.action.scan_and_delete_orphan_objects_action import ScanAndDeleteOrphanObjectsAction
from prime_backup.action.shrink_base_fileset_action import ShrinkBaseFilesetAction, NotBaseFileset
from prime_backup.exceptions import FilesetNotFound
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.types.file_info import FileListSummary


class PruneDatabaseTask(HeavyTask[None]):
	@property
	@override
	def id(self) -> str:
		return 'db_prune'

	@override
	def run(self) -> None:
		self.reply_tr('start')
		result = ScanAndDeleteOrphanObjectsAction().run()

		filesets = ListFilesetAction(is_base=True).run()
		fls_total = FileListSummary.zero()
		self.logger.info('Shrinking {} base filesets'.format(len(filesets)))
		for fileset in filesets:
			try:
				fls = ShrinkBaseFilesetAction(fileset.id).run()
			except (FilesetNotFound, NotBaseFileset):
				continue

			fls_total += fls
			if fls.count > 0:
				self.logger.debug('ShrinkBaseFilesetAction for {} done: {}'.format(fileset.id, fls))
			else:
				self.logger.debug('ShrinkBaseFilesetAction for {} done, noting to do'.format(fileset.id))
		self.logger.info('Shrank {} base filesets: {}'.format(len(filesets), fls_total))

		if result.total_orphan_count + fls_total.count == 0:
			self.reply_tr('done_clean')
		else:
			self.reply_tr('done', result.total_orphan_count, fls_total.count)
