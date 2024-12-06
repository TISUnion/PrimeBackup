import dataclasses
from typing import List

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.types.fileset_info import FilesetInfo


@dataclasses.dataclass(frozen=True)
class BadFilesetItem:
	fileset: FilesetInfo
	desc: str


@dataclasses.dataclass
class ValidateFilesetsResult:
	total: int = 0
	validated: int = 0
	ok: int = 0
	bad_filesets: List[BadFilesetItem] = dataclasses.field(default_factory=list)


class ValidateFilesetsAction(Action[ValidateFilesetsResult]):
	@override
	def is_interruptable(self) -> bool:
		return True

	def __validate(self, session: DbSession, result: ValidateFilesetsResult, filesets: List[FilesetInfo]):
		for fileset in filesets:
			if self.is_interrupted.is_set():
				break
			files = session.get_fileset_files(fileset.id)
			if fileset.file_object_count != len(files):
				result.bad_filesets.append(BadFilesetItem(fileset, 'fileset.file_object_count {} != actual file object count {}'.format(fileset.file_object_count, len(files))))

			# XXX: it's currently not possible to validate file size stats of delta fileset,
			#      since it's hard to get the associated base fileset of a delta fileset
			if fileset.is_base:
				file_count = 0
				file_raw_size_sum = 0
				file_stored_size_sum = 0
				for file in files:
					# NOTES: validation for the file roles are done in ValidateFilesAction
					file_count += 1
					file_raw_size_sum += file.blob_raw_size or 0
					file_stored_size_sum += file.blob_stored_size or 0

				if fileset.file_count != file_count:
					result.bad_filesets.append(BadFilesetItem(fileset, 'fileset.file_count {} != actual file count {}'.format(fileset.file_count, file_count)))
				elif fileset.raw_size != file_raw_size_sum:
					result.bad_filesets.append(BadFilesetItem(fileset, 'fileset.raw_size {} != actual file_raw_size_sum {}'.format(fileset.raw_size, file_raw_size_sum)))
				elif fileset.stored_size != file_stored_size_sum:
					result.bad_filesets.append(BadFilesetItem(fileset, 'fileset.stored_size {} != actual file_stored_size_sum {}'.format(fileset.stored_size, file_stored_size_sum)))

		bad_cnt = len({bad_item.fileset.id for bad_item in result.bad_filesets})
		result.total = len(filesets)
		result.validated = len(filesets)
		result.ok = len(filesets) - bad_cnt

	def run(self) -> ValidateFilesetsResult:
		self.logger.info('Fileset validation start')
		result = ValidateFilesetsResult()

		with DbAccess.open_session() as session:
			result.total = session.get_fileset_count()
			self.logger.info('Validating {} fileset objects'.format(result.total))
			for filesets in session.iterate_fileset_batch():
				if self.is_interrupted.is_set():
					break
				self.__validate(session, result, [FilesetInfo.of(fileset) for fileset in filesets])

		self.logger.info('Fileset validation done: total {}, validated {}, ok {}, bad {}'.format(
			result.total, result.validated, result.ok, len(result.bad_backups),
		))
		return result
