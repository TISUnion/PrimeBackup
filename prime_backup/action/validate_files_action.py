import dataclasses
from typing import List, Set, Tuple

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.db.values import FileRole
from prime_backup.types.blob_info import BlobInfo
from prime_backup.types.file_info import FileInfo


@dataclasses.dataclass(frozen=True)
class BadFileItem:
	file: FileInfo
	desc: str


@dataclasses.dataclass
class ValidateFilesResult:
	total: int = 0
	validated: int = 0
	ok: int = 0
	invalid: List[BadFileItem] = dataclasses.field(default_factory=list)
	bad_blob_relation: List[BadFileItem] = dataclasses.field(default_factory=list)
	bad_fileset_relation: List[BadFileItem] = dataclasses.field(default_factory=list)
	file_blob_mismatched: List[BadFileItem] = dataclasses.field(default_factory=list)


class ValidateFilesAction(Action[ValidateFilesResult]):
	@override
	def is_interruptable(self) -> bool:
		return True

	def __validate(self, session: DbSession, result: ValidateFilesResult, files: List[FileInfo]):
		def mark_bad(file_: FileInfo):
			bad_files.add((file_.fileset_id, file_.path))
		bad_files: Set[Tuple[int, str]] = set()  # set of (fileset_id, path)
		blob_hashes: Set[str] = set()
		fileset_ids: Set[int] = set()
		for file in files:
			if file.is_file():
				if file.blob is not None:
					blob_hashes.add(file.blob.hash)
				else:
					result.bad_blob_relation.append(BadFileItem(file, 'file without blob'))
					mark_bad(file)
			elif file.is_dir():
				if file.blob is not None:
					result.bad_blob_relation.append(BadFileItem(file, 'dir with blob'))
					mark_bad(file)
			elif file.is_link():
				if file.blob is not None:
					result.bad_blob_relation.append(BadFileItem(file, 'symlink with blob'))
					mark_bad(file)
				if len(file.content) == 0:
					result.invalid.append(BadFileItem(file, 'symlink without content'))
					mark_bad(file)

		hash_to_blob = session.get_blobs(sorted(blob_hashes))
		if self.is_interrupted.is_set():
			return
		filesets = session.get_filesets(sorted(fileset_ids))
		if self.is_interrupted.is_set():
			return

		for file in files:
			if (fileset := filesets.get(file.fileset_id)) is None:
				result.bad_fileset_relation.append(BadFileItem(file, f'fileset {file.fileset_id} does not exist'))
				mark_bad(file)
			elif fileset.is_base:
				if file.role != FileRole.standalone:
					result.bad_fileset_relation.append(BadFileItem(file, f'bad file role. fileset {file.fileset_id} is a base fileset, but file role is {file.role}'))
					mark_bad(file)
			else:  # fileset is not base, i.e. is a delta filesets
				if file.role not in [FileRole.delta_override, FileRole.delta_add, FileRole.delta_remove]:
					result.bad_fileset_relation.append(BadFileItem(file, f'bad file role. fileset {file.fileset_id} is a delta fileset, but file role is {file.role}'))
					mark_bad(file)

			file_blob: BlobInfo = file.blob
			if file.is_file() and file_blob is not None:
				blob = hash_to_blob.get(file_blob.hash)
				good_blob = False
				if blob is None:
					result.file_blob_mismatched.append(BadFileItem(file, f'file with missing blob {file_blob.hash}'))
				elif file_blob.hash != blob.hash:
					result.file_blob_mismatched.append(BadFileItem(file, f'mismatched blob data, blob hash should be {blob.hash}, but file blob hash is {file_blob.hash}'))
				elif file_blob.compress.name != blob.compress:
					result.file_blob_mismatched.append(BadFileItem(file, f'mismatched blob data, blob compress should be {blob.compress}, but file blob compress is {file_blob.compress.name}'))
				elif file_blob.raw_size != blob.raw_size:
					result.file_blob_mismatched.append(BadFileItem(file, f'mismatched blob data, blob raw_size should be {blob.raw_size}, but file blob raw_size is {file_blob.raw_size}'))
				elif file_blob.stored_size != blob.stored_size:
					result.file_blob_mismatched.append(BadFileItem(file, f'mismatched blob data, blob stored_size should be {blob.stored_size}, but file blob stored_size is {file_blob.stored_size}'))
				else:
					good_blob = True
				if not good_blob:
					mark_bad(file)

		result.validated += len(files)
		result.ok += len(files) - len(bad_files)

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
				if cnt % 50000 == 0 or cnt == result.total:
					self.logger.info('Validating {} / {} file objects'.format(cnt, result.total))
				self.__validate(session, result, [FileInfo.of(file) for file in files])

		self.logger.info('File validation done: total {}, validated {}, ok {}, bad {}'.format(
			result.total, result.validated, result.ok, result.validated - result.ok,
		))
		return result
