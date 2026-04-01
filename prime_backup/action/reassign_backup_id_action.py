import dataclasses
from typing import Optional

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.types.backup_filter import BackupSortOrder


@dataclasses.dataclass(frozen=True)
class ReassignBackupIdResult:
	max_id: Optional[int]


class ReassignBackupIdAction(Action[ReassignBackupIdResult]):
	def __init__(self, order: BackupSortOrder):
		super().__init__()
		self.order = order

	@override
	def run(self) -> ReassignBackupIdResult:
		with DbAccess.open_session() as session:
			max_id = session.reassign_backup_id(self.order)
		self.logger.debug('Backup ID reassigned, current max Backup.id: {}'.format(max_id))
		return ReassignBackupIdResult(max_id)
