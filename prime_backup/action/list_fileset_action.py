from typing import List, TypeVar, Optional

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.types.fileset_info import FilesetInfo

_T = TypeVar('_T')


class ListFilesetAction(Action[List[FilesetInfo]]):
	def __init__(self, is_base: Optional[bool] = None):
		super().__init__()
		self.is_base = is_base

	@override
	def run(self) -> List[FilesetInfo]:
		with DbAccess.open_session() as session:
			return [FilesetInfo.of(Fileset) for Fileset in session.list_fileset(is_base=self.is_base)]


class ListFilesetIdAction(Action[List[int]]):
	@override
	def run(self) -> List[int]:
		with DbAccess.open_session() as session:
			return session.list_fileset_ids()
