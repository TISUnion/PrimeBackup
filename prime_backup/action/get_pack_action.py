from abc import ABC, abstractmethod

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.exceptions import PackNameNotFound, PackNameNotUnique
from prime_backup.types.pack_info import PackInfo


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


class GetPackByNameAction(_GetPackActionBase):
	def __init__(self, pack_name: str):
		super().__init__()
		self.pack_name = pack_name

	@override
	def _do_get_pack(self, session: DbSession) -> schema.Pack:
		return session.get_pack_by_name(self.pack_name)


class GetPackByNamePrefixAction(_GetPackActionBase):
	def __init__(self, pack_name_prefix: str):
		super().__init__()
		self.pack_name_prefix = pack_name_prefix

	@override
	def _do_get_pack(self, session: DbSession) -> schema.Pack:
		matches = session.list_packs_by_name_prefix(self.pack_name_prefix)
		if len(matches) == 0:
			raise PackNameNotFound(self.pack_name_prefix)
		if len(matches) > 1:
			raise PackNameNotUnique(self.pack_name_prefix, sorted(map(PackInfo.of, matches), key=lambda p: p.name))
		return matches[0]
