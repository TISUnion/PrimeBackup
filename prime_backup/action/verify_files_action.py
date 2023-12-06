import dataclasses

from prime_backup.action import Action
from prime_backup.db.access import DbAccess


@dataclasses.dataclass
class VerifyFilesResult:
	total: int = 0


class VerifyFilesAction(Action):
	def run(self) -> VerifyFilesResult:
		with DbAccess.open_session() as session:
			raise NotImplementedError()
