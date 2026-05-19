from abc import ABC, abstractmethod
from typing import List

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.exceptions import PackFileNameNotFound, PackFileNameNotUnique
from prime_backup.types.pack_info import PackInfo
from prime_backup.utils import pack_utils


class _GetPackActionBase(Action[PackInfo], ABC):
	@override
	def run(self) -> PackInfo:
		with DbAccess.open_session() as session:
			pack = self._do_get_pack(session)
			return PackInfo.of(pack)

	@abstractmethod
	def _do_get_pack(self, session: DbSession) -> schema.Pack:
		...


class GetPackByIdAction(_GetPackActionBase):
	def __init__(self, pack_id: int):
		super().__init__()
		self.pack_id = pack_id

	@override
	def _do_get_pack(self, session: DbSession) -> schema.Pack:
		return session.get_pack_by_id(self.pack_id)


class GetPackByFileNamePrefixAction(_GetPackActionBase):
	def __init__(self, pack_file_name_prefix: str):
		super().__init__()
		self.pack_file_name_prefix = pack_file_name_prefix

	@override
	def _do_get_pack(self, session: DbSession) -> schema.Pack:
		matched_pack_ids: List[int] = []
		for pack_id in session.get_all_pack_ids():
			if pack_utils.get_pack_file_name(pack_id).startswith(self.pack_file_name_prefix):
				matched_pack_ids.append(pack_id)
				if len(matched_pack_ids) >= 3:
					break
		if len(matched_pack_ids) == 0:
			raise PackFileNameNotFound(self.pack_file_name_prefix)
		if len(matched_pack_ids) > 1:
			packs = session.get_packs_by_ids(matched_pack_ids).values()
			candidates = sorted((PackInfo.of(pack) for pack in packs if pack is not None), key=lambda pack: pack.file_name)
			raise PackFileNameNotUnique(self.pack_file_name_prefix, candidates)
		return session.get_pack_by_id(matched_pack_ids[0])
