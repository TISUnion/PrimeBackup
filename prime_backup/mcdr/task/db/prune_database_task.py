from typing_extensions import override

from prime_backup.action.list_fileset_action import ListFilesetAction
from prime_backup.action.scan_and_delete_orphan_objects_action import ScanAndDeleteOrphanObjectsAction
from prime_backup.action.shrink_base_fileset_action import ShrinkBaseFilesetAction
from prime_backup.exceptions import FilesetNotFound
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.types.file_info import FileListSummary


class PruneDatabaseTask(HeavyTask[None]):
	@property
	@override
	def id(self) -> str:
		return 'db_prune_database'

	@override
	def run(self) -> None:
		self.reply('ScanAndDeleteOrphanObjectsAction start')
		result = ScanAndDeleteOrphanObjectsAction().run()
		self.reply('ScanAndDeleteOrphanObjectsAction done: {}'.format(result))

		filesets = ListFilesetAction().run()
		fls_total = FileListSummary.zero()
		self.reply('Running ShrinkBaseFilesetAction for {} filesets'.format(len(filesets)))
		for fileset in filesets:
			if fileset.is_base:
				try:
					fls = ShrinkBaseFilesetAction(fileset.id).run()
				except FilesetNotFound:
					continue

				fls_total += fls
				if fls.count > 0:
					self.reply('ShrinkBaseFilesetAction for {} done: {}'.format(fileset.id, fls))
				else:
					self.reply('ShrinkBaseFilesetAction for {} done, noting to do'.format(fileset.id))

		if result.total_orphan_count + fls_total.count == 0:
			self.reply('Prune 0 stuff, the database is already clean')
		else:
			self.reply('Prune done, {} {}'.format(result, fls_total))
