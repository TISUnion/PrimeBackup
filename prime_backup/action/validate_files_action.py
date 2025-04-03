import dataclasses
import enum
from typing import List, Set, Dict, Tuple

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.db.values import FileRole
from prime_backup.types.blob_info import BlobInfo
from prime_backup.types.file_info import FileInfo, FileUniqueKey
from prime_backup.types.fileset_info import FilesetInfo


class BadFileItemType(enum.Enum):
	invalid = enum.auto()
	bad_blob_relation= enum.auto()
	bad_fileset_relation= enum.auto()
	file_blob_mismatched= enum.auto()


@dataclasses.dataclass(frozen=True)
class BadFileItem:
	file: FileInfo
	errors: Dict[BadFileItemType, str] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class ValidateFilesResult:
	total: int = 0
	validated: int = 0
	bad_files: Dict[FileUniqueKey, BadFileItem] = dataclasses.field(default_factory=dict)

	@property
	def ok(self) -> int:
		return self.validated - len(self.bad_files)

	@property
	def bad(self) -> int:
		return len(self.bad_files)

	def add_bad(self, file: FileInfo, typ: BadFileItemType, msg: str):
		if file.unique_key not in self.bad_files:
			self.bad_files[file.unique_key] = BadFileItem(file=file)
		self.bad_files[file.unique_key].errors[typ] = msg

	def get_bad_by_type(self, typ: BadFileItemType) -> List[Tuple[FileInfo, str]]:
		result = []
		for bad_file in self.bad_files.values():
			if (msg := bad_file.errors.get(typ)) is not None:
				result.append((bad_file.file, msg))
		return result


class ValidateFilesAction(Action[ValidateFilesResult]):
	@override
	def is_interruptable(self) -> bool:
		return True

	def __validate(self, session: DbSession, result: ValidateFilesResult, files: List[FileInfo]):
		blob_hashes: Set[str] = set()
		fileset_ids: Set[int] = set()
		for file in files:
			fileset_ids.add(file.fileset_id)
			if file.is_file():
				if file.blob is not None:
					blob_hashes.add(file.blob.hash)
				else:
					result.add_bad(file, BadFileItemType.bad_blob_relation, 'file without blob')
			elif file.is_dir():
				if file.blob is not None:
					result.add_bad(file, BadFileItemType.bad_blob_relation, 'dir with blob')
			elif file.is_link():
				if file.blob is not None:
					result.add_bad(file, BadFileItemType.bad_blob_relation, 'symlink with blob')
				if len(file.content) == 0:
					result.add_bad(file, BadFileItemType.invalid, 'symlink without content')

		hash_to_blob = session.get_blobs(sorted(blob_hashes))
		if self.is_interrupted.is_set():
			return
		filesets = session.get_filesets(sorted(fileset_ids))
		if self.is_interrupted.is_set():
			return

		# TODO: check orphan
		for file in files:
			if (fileset := filesets.get(file.fileset_id)) is None:
				result.add_bad(file, BadFileItemType.bad_fileset_relation, f'fileset {file.fileset_id} does not exist')
			elif FilesetInfo.of(fileset).is_base:
				if file.role != FileRole.standalone:
					result.add_bad(file, BadFileItemType.bad_fileset_relation, f'bad file role. fileset {file.fileset_id} is a base fileset, but file role is {file.role}')
			else:  # fileset is not base, i.e. is a delta filesets
				if file.role not in [FileRole.delta_override, FileRole.delta_add, FileRole.delta_remove]:
					result.add_bad(file, BadFileItemType.bad_fileset_relation, f'bad file role. fileset {file.fileset_id} is a delta fileset, but file role is {file.role}')

			file_blob: BlobInfo = file.blob
			if file.is_file() and file_blob is not None:
				blob = hash_to_blob.get(file_blob.hash)
				if blob is None:
					result.add_bad(file, BadFileItemType.file_blob_mismatched, f'file with missing blob {file_blob.hash}')
				elif file_blob.hash != blob.hash:
					result.add_bad(file, BadFileItemType.file_blob_mismatched, f'mismatched blob data, blob hash should be {blob.hash}, but file blob hash is {file_blob.hash}')
				elif file_blob.compress.name != blob.compress:
					result.add_bad(file, BadFileItemType.file_blob_mismatched, f'mismatched blob data, blob compress should be {blob.compress}, but file blob compress is {file_blob.compress.name}')
				elif file_blob.raw_size != blob.raw_size:
					result.add_bad(file, BadFileItemType.file_blob_mismatched, f'mismatched blob data, blob raw_size should be {blob.raw_size}, but file blob raw_size is {file_blob.raw_size}')
				elif file_blob.stored_size != blob.stored_size:
					result.add_bad(file, BadFileItemType.file_blob_mismatched, f'mismatched blob data, blob stored_size should be {blob.stored_size}, but file blob stored_size is {file_blob.stored_size}')

		result.validated += len(files)

	@override
	def run(self) -> ValidateFilesResult:
		self.logger.info('File validation start')
		result = ValidateFilesResult()

		with DbAccess.open_session() as session:
			result.total = session.get_file_object_count()
			cnt = 0
			for files in session.iterate_file_batch():
				if self.is_interrupted.is_set():
					break
				cnt += len(files)
				if cnt % 20000 == 0 or cnt == result.total:
					self.logger.info('Validating {} / {} file objects'.format(cnt, result.total))
				self.__validate(session, result, [FileInfo.of(file) for file in files])

		self.logger.info('File validation done: total {}, validated {}, ok {}, bad {}'.format(
			result.total, result.validated, result.ok, result.validated - result.ok,
		))
		return result
