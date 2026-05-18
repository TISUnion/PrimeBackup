import contextlib
import dataclasses
from pathlib import Path
from typing import Collection, List, Optional

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.action.helpers.pack_reader import PackReader
from prime_backup.action.helpers.pack_writer import PackWriter
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.exceptions import PackIdNotFound
from prime_backup.types.pack_info import PackChangeSummary, PackInfo
from prime_backup.utils import collection_utils


@dataclasses.dataclass(frozen=True)
class CollectPacksForCompactResult:
	pack_ids: List[int]

	@property
	def count(self) -> int:
		return len(self.pack_ids)


class CollectPacksForCompactStep(Action[CollectPacksForCompactResult]):
	def __init__(self, session: DbSession, *, pack_ids: Optional[Collection[int]] = None, threshold: float, raise_if_not_found: bool = True):
		super().__init__()
		self.session = session
		self.pack_ids = None if pack_ids is None else collection_utils.deduplicated_list(pack_ids)
		self.threshold = threshold
		self.raise_if_not_found = raise_if_not_found

	@override
	def run(self) -> CollectPacksForCompactResult:
		if self.threshold < 0:
			raise ValueError('negative pack compact threshold {}'.format(self.threshold))

		result: List[int] = []
		if self.pack_ids is None:
			packs = self.session.list_packs()
		else:
			packs = []
			for pack_id, pack in self.session.get_packs_by_ids(self.pack_ids).items():
				if pack is None:
					if self.raise_if_not_found:
						raise PackIdNotFound(pack_id)
					self.logger.warning('Pack id {} does not exist, skipped compact collection'.format(pack_id))
					continue
				packs.append(pack)

		for pack in packs:
			if pack.size <= 0:
				if pack.live_count <= 0:
					result.append(pack.id)
				continue
			if pack.live_size < pack.size and pack.live_size / pack.size < self.threshold:
				result.append(pack.id)

		return CollectPacksForCompactResult(result)


class CompactPacksStep(Action[PackChangeSummary]):
	def __init__(self, session: DbSession, pack_ids: Collection[int], *, raise_if_not_found: bool = True):
		super().__init__()
		self.session = session
		self.pack_ids = collection_utils.deduplicated_list(pack_ids)
		self.raise_if_not_found = raise_if_not_found

	@override
	def run(self) -> PackChangeSummary:
		summary = PackChangeSummary.zero()
		old_pack_paths: List[Path] = []
		pack_writer: Optional[PackWriter] = None
		new_pack_paths: List[Path] = []

		try:
			packs_by_id = self.session.get_packs_by_ids(self.pack_ids)
			for pack_id in self.pack_ids:
				pack = packs_by_id.get(pack_id)
				if pack is None:
					if self.raise_if_not_found:
						raise PackIdNotFound(pack_id)
					self.logger.warning('Pack id {} does not exist, skipped compact'.format(pack_id))
					continue

				if pack.live_size == pack.size and pack.live_count == pack.count:
					continue

				old_pack_info = PackInfo.of(pack)
				live_entries = self.session.get_live_entries_by_pack_id(pack.id)
				if len(live_entries) == 0:
					self.logger.info('Removing empty pack id={} name={} size={}'.format(pack.id, pack.name, pack.size))
					self.session.delete_pack(pack)
					summary.removed_pack_count += 1
					summary.old_size += old_pack_info.size
					old_pack_paths.append(old_pack_info.file_path)
					continue

				if pack_writer is None:
					pack_writer = PackWriter(self.session)

				self.logger.info('Compacting pack id={} name={} live={}/{} entries={}/{}'.format(
					pack.id, pack.name, pack.live_size, pack.size, pack.live_count, pack.count,
				))
				for entry in live_entries:
					with PackReader.open_entry(pack.name, entry.offset, entry.size) as reader:
						entry_location = pack_writer.write_entry_from_reader(reader, entry.size)
					self.session.move_pack_entry(entry, entry_location)
				self.session.flush()

				self.session.delete_pack(pack)
				summary.compacted_pack_count += 1
				summary.old_size += old_pack_info.size
				old_pack_paths.append(old_pack_info.file_path)

			if pack_writer is not None:
				pack_writer.close()
				new_pack_paths = pack_writer.get_rollback_paths()
				summary.new_size += pack_writer.get_created_pack_summary().new_size

			self.session.commit()
		except Exception:
			if pack_writer is not None:
				with contextlib.suppress(Exception):
					pack_writer.close()
				if not new_pack_paths:
					new_pack_paths = pack_writer.get_rollback_paths()
			for new_pack_path in new_pack_paths:
				try:
					new_pack_path.unlink(missing_ok=True)
				except OSError as e:
					self.logger.warning('Failed to delete rollback pack file {!r}: {}'.format(new_pack_path, e))
			raise

		for old_pack_path in old_pack_paths:
			try:
				old_pack_path.unlink(missing_ok=True)
			except OSError as e:
				self.logger.warning('Failed to delete compacted old pack file {!r}; it can be removed by database prune later: {}'.format(old_pack_path, e))

		if summary.touched_pack_count > 0:
			self.logger.info('Pack compact done, touched {} packs, old_size={}, new_size={}, freed={}'.format(
				summary.touched_pack_count, summary.old_size, summary.new_size, summary.freed_size,
			))
		return summary


class CompactAllPacksAction(Action[PackChangeSummary]):
	def __init__(self, *, threshold: float):
		super().__init__()
		self.threshold = threshold

	@override
	def run(self) -> PackChangeSummary:
		with DbAccess.open_session() as session:
			pack_ids = CollectPacksForCompactStep(session, pack_ids=None, threshold=self.threshold).run().pack_ids
			if len(pack_ids) == 0:
				return PackChangeSummary.zero()
			return CompactPacksStep(session, pack_ids).run()
