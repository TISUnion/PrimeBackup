from prime_backup.action.vacuum_sqlite_action import VacuumSqliteAction
from prime_backup.mcdr.task.basic_tasks import OperationTask
from prime_backup.types.units import ByteCount


class VacuumSqliteTask(OperationTask):
	@property
	def name(self) -> str:
		return 'vacuum_sqlite'

	def run(self) -> None:
		self.logger.info('Compacting SQLite database')
		diff = VacuumSqliteAction().run()
		self.logger.info('Database compaction complete, size change: {} -> {} ({})'.format(
			ByteCount(diff.before).auto_str(), ByteCount(diff.after).auto_str(), ByteCount(diff.diff).auto_str(),
		))
