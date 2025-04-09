from typing import List, TypeVar

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.types.fileset_info import FilesetInfo

_T = TypeVar('_T')


class ListFilesetAction(Action[List[FilesetInfo]]):
	@override
	def run(self) -> List[FilesetInfo]:
		with DbAccess.open_session() as session:
			return [FilesetInfo.of(Fileset) for Fileset in session.list_fileset()]


class ListFilesetIdAction(Action[List[int]]):
	@override
	def run(self) -> List[int]:
		with DbAccess.open_session() as session:
			return session.list_fileset_ids()
