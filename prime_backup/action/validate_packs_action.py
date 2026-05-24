import contextlib
import dataclasses
import enum
from typing import Dict, List, Optional

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.types.pack_info import PackInfo


class BadPackItemType(enum.Enum):
	missing_file = enum.auto()
	mismatched = enum.auto()


@dataclasses.dataclass(frozen=True)
class BadPackItem:
	pack: PackInfo
	typ: BadPackItemType
	desc: str


@dataclasses.dataclass
class ValidatePacksResult:
	total: int = 0
	validated: int = 0
	ok: int = 0
	bad_packs: List[BadPackItem] = dataclasses.field(default_factory=list)

	@property
	def bad(self) -> int:
		return len(self.bad_packs)

	def add_bad(self, pack: PackInfo, typ: BadPackItemType, msg: str):
		self.bad_packs.append(BadPackItem(pack, typ, msg))

	def group_bad_by_type(self) -> Dict[BadPackItemType, List[BadPackItem]]:
		result: Dict[BadPackItemType, List[BadPackItem]] = {}
		for bad_pack in self.bad_packs:
			result.setdefault(bad_pack.typ, []).append(bad_pack)
		return result


class ValidatePacksAction(Action[ValidatePacksResult]):
	@override
	def is_interruptable(self) -> bool:
		return True

	def __validate(self, session: DbSession, result: ValidatePacksResult, packs: List[PackInfo]):
		pack_live_stats = session.get_pack_live_stats_by_ids([pack.id for pack in packs])
		for pack in packs:
			if self.is_interrupted.is_set():
				break
			result.validated += 1

			pack_path = pack.file_path
			if not pack_path.is_file():
				result.add_bad(pack, BadPackItemType.missing_file, f'pack file {pack_path} does not exist')
				continue

			file_size = pack_path.stat().st_size
			if file_size != pack.size:
				result.add_bad(pack, BadPackItemType.mismatched, f'pack file size mismatch, expect {pack.size}, found {file_size}')
				continue

			if pack.live_size > pack.size:
				result.add_bad(pack, BadPackItemType.mismatched, f'live_size {pack.live_size} is larger than size {pack.size}')
				continue
			if pack.live_entry_count > pack.entry_count:
				result.add_bad(pack, BadPackItemType.mismatched, f'live_entry_count {pack.live_entry_count} is larger than entry_count {pack.entry_count}')
				continue

			live_stats = pack_live_stats[pack.id]
			if pack.live_size != live_stats.live_size:
				result.add_bad(pack, BadPackItemType.mismatched, f'live_size mismatch, expect {live_stats.live_size}, found {pack.live_size}')
				continue
			if pack.live_entry_count != live_stats.live_entry_count:
				result.add_bad(pack, BadPackItemType.mismatched, f'live_entry_count mismatch, expect {live_stats.live_entry_count}, found {pack.live_entry_count}')
				continue

			result.ok += 1

	@override
	def run(self, *, session: Optional[DbSession] = None) -> ValidatePacksResult:
		self.logger.info('Pack validation start')
		result = ValidatePacksResult()

		with contextlib.ExitStack() as es:
			if session is None:
				session = es.enter_context(DbAccess.open_session())

			result.total = session.get_pack_count()
			cnt = 0
			for packs in session.iterate_pack_batch(batch_size=3000):
				if self.is_interrupted.is_set():
					break
				cnt += len(packs)
				self.logger.info('Validating {} / {} packs'.format(cnt, result.total))
				self.__validate(session, result, list(map(PackInfo.of, packs)))

		self.logger.info('Pack validation done: total {}, validated {}, ok {}, bad {}'.format(
			result.total, result.validated, result.ok, len(result.bad_packs),
		))
		return result
