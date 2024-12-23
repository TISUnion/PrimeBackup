import dataclasses
from typing import List, Set

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.types.backup_info import BackupInfo


@dataclasses.dataclass(frozen=True)
class BadBackupItem:
	backup: BackupInfo
	desc: str


@dataclasses.dataclass
class ValidateBackupsResult:
	total: int = 0
	validated: int = 0
	bad_backups: List[BadBackupItem] = dataclasses.field(default_factory=list)

	@property
	def ok(self) -> int:
		return self.validated - len(self.bad_backups)

	@property
	def bad(self) -> int:
		return len(self.bad_backups)
	
	def add_bad(self, backup: BackupInfo, msg: str):
		self.bad_backups.append(BadBackupItem(backup, msg))


class ValidateBackupsAction(Action[ValidateBackupsResult]):
	@override
	def is_interruptable(self) -> bool:
		return True

	def __validate(self, session: DbSession, result: ValidateBackupsResult, backups: List[BackupInfo]):
		fileset_ids: Set[int] = set()
		for backup in backups:
			fileset_ids.add(backup.fileset_id_base)
			fileset_ids.add(backup.fileset_id_delta)
		if self.is_interrupted.is_set():
			return
		filesets = session.get_filesets(sorted(fileset_ids))

		for backup in backups:
			result.validated += 1
			if (fileset_base := filesets.get(backup.fileset_id_base)) is None:
				result.add_bad(backup, 'base fileset {} does not exist'.format(backup.fileset_id_base))
			elif (fileset_delta := filesets.get(backup.fileset_id_delta)) is None:
				result.add_bad(backup, 'delta fileset {} does not exist'.format(backup.fileset_id_delta))
			elif backup.file_count != fileset_base.file_count + fileset_delta.file_count:
				result.add_bad(backup, 'mismatched file count, backup {} != {} + {} (base + delta)'.format(
					backup.file_count, fileset_base.file_count, fileset_delta.file_count,
				))
			elif backup.raw_size != fileset_base.file_raw_size_sum + fileset_delta.file_raw_size_sum:
				result.add_bad(backup, 'mismatched raw size, backup {} != {} + {} (base + delta)'.format(
					backup.raw_size, fileset_base.file_raw_size_sum, fileset_delta.file_raw_size_sum,
				))
			elif backup.stored_size != fileset_base.file_stored_size_sum + fileset_delta.file_stored_size_sum:
				result.add_bad(backup, 'mismatched stored size, backup {} != {} + {} (base + delta)'.format(
					backup.stored_size, fileset_base.file_stored_size_sum, fileset_delta.file_stored_size_sum,
				))

	def run(self) -> ValidateBackupsResult:
		self.logger.info('Backup validation start')
		result = ValidateBackupsResult()

		with DbAccess.open_session() as session:
			result.total = session.get_backup_count()
			self.logger.info('Validating {} backup objects'.format(result.total))
			for backups in session.iterate_backup_batch():
				if self.is_interrupted.is_set():
					break
				self.__validate(session, result, [BackupInfo.of(backup) for backup in backups])

		self.logger.info('Backup validation done: total {}, validated {}, ok {}, bad {}'.format(
			result.total, result.validated, result.ok, len(result.bad_backups),
		))
		return result
