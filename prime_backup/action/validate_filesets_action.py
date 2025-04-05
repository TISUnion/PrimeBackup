import dataclasses
import functools
from typing import List, Dict, Set

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.db.values import FileRole
from prime_backup.types.fileset_info import FilesetInfo


@dataclasses.dataclass(frozen=True)
class BadFilesetItem:
	fileset: FilesetInfo
	descriptions: List[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class ValidateFilesetsResult:
	total: int = 0
	validated: int = 0
	bad_filesets: Dict[int, BadFilesetItem] = dataclasses.field(default_factory=dict)
	affected_backup_ids: List[int] = dataclasses.field(default_factory=list)

	@property
	def ok(self) -> int:
		return self.validated - len(self.bad_filesets)

	@property
	def bad(self) -> int:
		return len(self.bad_filesets)

	def add_bad(self, fileset: FilesetInfo, msg: str):
		if fileset.id not in self.bad_filesets:
			self.bad_filesets[fileset.id] = BadFilesetItem(fileset=fileset)
		self.bad_filesets[fileset.id].descriptions.append(msg)


class ValidateFilesetsAction(Action[ValidateFilesetsResult]):
	@override
	def is_interruptable(self) -> bool:
		return True

	def __validate(self, session: DbSession, result: ValidateFilesetsResult, filesets: List[FilesetInfo]):
		@functools.lru_cache(maxsize=16)
		def get_fileset_files_cached(fileset_id_: int):
			return session.get_fileset_files(fileset_id_)

		base_filesets: Dict[int, FilesetInfo] = {}
		fileset_ids_to_query: Set[int] = set()
		for fileset in filesets:
			if fileset.is_base:
				base_filesets[fileset.id] = fileset
			else:
				fileset_ids_to_query.add(fileset.base_id)
		fileset_ids_to_query -= base_filesets.keys()

		for fileset in map(FilesetInfo.of, session.get_filesets(sorted(fileset_ids_to_query)).values()):
			base_filesets[fileset.id] = fileset

		orphan_fileset_ids = set(session.filtered_orphan_fileset_ids([fileset.id for fileset in filesets]))

		for fileset in filesets:
			if self.is_interrupted.is_set():
				break
			result.validated += 1
			if fileset.id in orphan_fileset_ids:
				result.add_bad(fileset, f'orphan fileset with 0 associated backup')

			if fileset.id <= 0:
				result.add_bad(fileset, 'unexpected fileset id {}, should not <= 0'.format(fileset.id))
				continue

			files = get_fileset_files_cached(fileset.id)
			if fileset.file_object_count != len(files):
				result.add_bad(fileset, 'fileset.file_object_count {} != actual file object count {}'.format(fileset.file_object_count, len(files)))
				continue

			if not fileset.is_base:
				base_fileset = base_filesets.get(fileset.base_id)
				if base_fileset is None:
					result.add_bad(fileset, 'base fileset {} does not exist'.format(fileset.base_id))
					continue
				elif not base_fileset.is_base:
					result.add_bad(fileset, 'base fileset {} is not a base fileset, its base_id == {}'.format(fileset.base_id, base_fileset.base_id))
					continue

			# NOTES: validation for the file roles are done in ValidateFilesAction
			file_count = 0
			file_raw_size_sum = 0
			file_stored_size_sum = 0
			calc_file_stats_ok = True
			if fileset.is_base:
				for file in files:
					file_count += 1
					file_raw_size_sum += file.blob_raw_size or 0
					file_stored_size_sum += file.blob_stored_size or 0
			else:
				if self.is_interrupted.is_set():
					break
				base_files_by_path = {file.path: file for file in get_fileset_files_cached(fileset.base_id)}
				for file in files:
					old_file = base_files_by_path.get(file.path)
					if file.role == FileRole.delta_add:
						if old_file is not None:
							result.add_bad(fileset, 'file {!r} role is delta_add, but it exists in the base fileset {}'.format(file.path, fileset.base_id))
							calc_file_stats_ok = False
							break
						file_count += 1
						file_raw_size_sum += file.blob_raw_size or 0
						file_stored_size_sum += file.blob_stored_size or 0
					elif file.role == FileRole.delta_override:
						if old_file is None:
							result.add_bad(fileset, 'file {!r} role is delta_override, but it does not exist in the base fileset {}'.format(file.path, fileset.base_id))
							calc_file_stats_ok = False
							break
						file_raw_size_sum += (file.blob_raw_size or 0) - (old_file.blob_raw_size or 0)
						file_stored_size_sum += (file.blob_stored_size or 0) - (old_file.blob_stored_size or 0)
					elif file.role == FileRole.delta_remove:
						if old_file is None:
							result.add_bad(fileset, 'file {!r} role is delta_remove, but it does not exist in the base fileset {}'.format(file.path, fileset.base_id))
							calc_file_stats_ok = False
							break
						file_count -= 1
						file_raw_size_sum -= old_file.blob_raw_size or 0
						file_stored_size_sum -= old_file.blob_stored_size or 0

			if calc_file_stats_ok:
				if fileset.file_count != file_count:
					result.add_bad(fileset, 'fileset.file_count {} != actual file count {}'.format(fileset.file_count, file_count))
				elif fileset.raw_size != file_raw_size_sum:
					result.add_bad(fileset, 'fileset.raw_size {} != actual file_raw_size_sum {}'.format(fileset.raw_size, file_raw_size_sum))
				elif fileset.stored_size != file_stored_size_sum:
					result.add_bad(fileset, 'fileset.stored_size {} != actual file_stored_size_sum {}'.format(fileset.stored_size, file_stored_size_sum))

	def run(self) -> ValidateFilesetsResult:
		self.logger.info('Fileset validation start')
		result = ValidateFilesetsResult()

		with DbAccess.open_session() as session:
			result.total = session.get_fileset_count()
			self.logger.info('Validating {} fileset objects'.format(result.total))
			cnt = 0
			for filesets in session.iterate_fileset_batch(batch_size=200):
				if self.is_interrupted.is_set():
					break
				cnt += len(filesets)
				self.logger.info('Validating {} / {} fileset objects'.format(cnt, result.total))
				self.__validate(session, result, [FilesetInfo.of(fileset) for fileset in filesets])

			result.affected_backup_ids = session.get_backup_ids_by_fileset_ids(list(result.bad_filesets.keys()))

		self.logger.info('Fileset validation done: total {}, validated {}, ok {}, bad {}'.format(
			result.total, result.validated, result.ok, len(result.bad_filesets),
		))
		return result
