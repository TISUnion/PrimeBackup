import collections
import dataclasses
from typing import List, Dict

from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
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
	file_blob_mismatched: List[BadFileItem] = dataclasses.field(default_factory=list)


class ValidateFilesAction(Action[ValidateFilesResult]):
	def is_interruptable(self) -> bool:
		return True

	def __validate(self, session: DbSession, result: ValidateFilesResult, files: List[FileInfo]):
		hash_to_file: Dict[str, List[FileInfo]] = collections.defaultdict(list)
		for file in files:
			if self.is_interrupted.is_set():
				break

			result.validated += 1

			if file.is_file():
				if file.blob is not None:
					hash_to_file[file.blob.hash].append(file)
				else:
					result.bad_blob_relation.append(BadFileItem(file, 'file without blob'))
					continue
			elif file.is_dir():
				if file.blob is not None:
					result.bad_blob_relation.append(BadFileItem(file, 'dir with blob'))
					continue
				result.ok += 1
			elif file.is_link():
				if file.blob is not None:
					result.bad_blob_relation.append(BadFileItem(file, 'symlink with blob'))
					continue
				if len(file.content) == 0:
					result.invalid.append(BadFileItem(file, 'symlink without content'))
					continue
				result.ok += 1
			else:
				result.ok += 1

		hash_to_blob = session.get_blobs(list(hash_to_file.keys()))
		for h, files in hash_to_file.items():
			blob = hash_to_blob[h]
			for file in files:
				file_blob: BlobInfo = file.blob
				if file_blob is None:
					raise AssertionError(f'file.blob is None, hash={h}, file={file}')
				if blob is None:
					result.file_blob_mismatched.append(BadFileItem(file, f'file with missing blob {h}'))
				elif file_blob.hash != blob.hash:
					result.file_blob_mismatched.append(BadFileItem(file, f'mismatched blob data, blob hash should be {blob.hash}, but file blob hash is {file_blob.hash}'))
				elif file_blob.compress.name != blob.compress:
					result.file_blob_mismatched.append(BadFileItem(file, f'mismatched blob data, blob compress should be {blob.compress}, but file blob compress is {file_blob.compress.name}'))
				elif file_blob.raw_size != blob.raw_size:
					result.file_blob_mismatched.append(BadFileItem(file, f'mismatched blob data, blob raw_size should be {blob.raw_size}, but file blob raw_size is {file_blob.raw_size}'))
				elif file_blob.stored_size != blob.stored_size:
					result.file_blob_mismatched.append(BadFileItem(file, f'mismatched blob data, blob stored_size should be {blob.stored_size}, but file blob stored_size is {file_blob.stored_size}'))
				else:
					result.ok += 1

	def run(self) -> ValidateFilesResult:
		self.logger.info('File validation start')
		result = ValidateFilesResult()

		with DbAccess.open_session() as session:
			result.total = session.get_file_count()
			cnt = 0
			for files in session.iterate_file_batch():
				if self.is_interrupted.is_set():
					break
				cnt += len(files)
				if cnt % 50000 == 0 or cnt == result.total:
					self.logger.info('Validating {} / {} files'.format(cnt, result.total))
				self.__validate(session, result, [FileInfo.of(file) for file in files])

		self.logger.info('File validation done: total {}, validated {}, ok {}, bad {}'.format(
			result.total, result.validated, result.ok, result.validated - result.ok,
		))
		return result
