import dataclasses
from typing import List, Dict

from prime_backup.action import Action
from prime_backup.compressors import Compressor
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.types.blob_info import BlobInfo
from prime_backup.types.file_info import FileInfo
from prime_backup.utils import blob_utils, hash_utils
from prime_backup.utils.thread_pool import FailFastThreadPool


@dataclasses.dataclass(frozen=True)
class BadBlobItem:
	blob: BlobInfo
	desc: str


@dataclasses.dataclass
class ValidateBlobsResult:
	total: int = 0
	validated: int = 0
	ok: int = 0
	invalid: List[BadBlobItem] = dataclasses.field(default_factory=list)  # wierd blobs
	missing: List[BadBlobItem] = dataclasses.field(default_factory=list)  # the file of the blob is missing
	corrupted: List[BadBlobItem] = dataclasses.field(default_factory=list)  # decompress failed
	mismatched: List[BadBlobItem] = dataclasses.field(default_factory=list)  # hash mismatch
	orphan: List[BadBlobItem] = dataclasses.field(default_factory=list)  # orphan blobs

	affected_file_count: int = 0
	affected_file_samples: List[FileInfo] = dataclasses.field(default_factory=list)
	affected_backup_ids: List[int] = 0


class ValidateBlobsAction(Action[ValidateBlobsResult]):
	def is_interruptable(self) -> bool:
		return True

	def __validate(self, session: DbSession, result: ValidateBlobsResult, blobs: List[BlobInfo]):
		hash_to_blobs: Dict[str, BlobInfo] = {}  # store "good" blobs only

		def validate_one_blob(blob: BlobInfo):
			blob_path = blob_utils.get_blob_path(blob.hash)

			if not blob_path.is_file():
				result.missing.append(BadBlobItem(blob, 'blob file does not exist'))
				return

			try:
				# Notes: There are some codes that use `CompressMethod[blob.compress]`,
				# which might fail hard if the blob.compress is invalid.
				# Maybe we need to make them fail-proof somehow?
				compressor = Compressor.create(blob.compress)
			except ValueError:
				result.invalid.append(BadBlobItem(blob, f'unknown compress method {blob.compress!r}'))
				return

			try:
				with compressor.open_decompressed_bypassed(blob_path) as (reader, f_decompressed):
					sah = hash_utils.calc_reader_size_and_hash(f_decompressed)
			except Exception as e:
				result.corrupted.append(BadBlobItem(blob, f'cannot read and decompress blob file: ({type(e)} {e}'))
				return

			file_size = reader.get_read_len()
			if file_size != blob.stored_size:
				result.mismatched.append(BadBlobItem(blob, f'stored size mismatch, expect {blob.stored_size}, found {file_size}'))
				return
			if sah.hash != blob.hash:
				result.mismatched.append(BadBlobItem(blob, f'hash mismatch, expect {blob.hash}, found {sah.hash}'))
				return
			if sah.size != blob.raw_size:
				result.mismatched.append(BadBlobItem(blob, f'raw size mismatch, expect {blob.raw_size}, found {sah.size}'))
				return

			# it's a good blob
			hash_to_blobs[blob.hash] = blob

		with FailFastThreadPool('validator') as pool:
			for b in blobs:
				if self.is_interrupted.is_set():
					break
				result.validated += 1
				pool.submit(validate_one_blob, b)

		orphan_hashes = set(session.filtered_orphan_blob_hashes(list(hash_to_blobs.keys())))
		for h, b in hash_to_blobs.items():
			if h in orphan_hashes:
				result.orphan.append(BadBlobItem(b, f'orphan blob with 0 associated file, hash {h}'))
			else:
				result.ok += 1

	def run(self) -> ValidateBlobsResult:
		self.logger.info('Blob validation start')
		result = ValidateBlobsResult()

		with DbAccess.open_session() as session:
			result.total = session.get_blob_count()
			cnt = 0
			for blobs in session.iterate_blob_batch():
				if self.is_interrupted.is_set():
					break
				cnt += len(blobs)
				self.logger.info('Validating {} / {} blobs'.format(cnt, result.total))
				self.__validate(session, result, list(map(BlobInfo.of, blobs)))

			bad_blob_hashes = []
			bad_blob_hashes.extend([bbi.blob.hash for bbi in result.invalid])
			bad_blob_hashes.extend([bbi.blob.hash for bbi in result.missing])
			bad_blob_hashes.extend([bbi.blob.hash for bbi in result.corrupted])
			bad_blob_hashes.extend([bbi.blob.hash for bbi in result.mismatched])
			bad_blob_hashes.extend([bbi.blob.hash for bbi in result.orphan])
			if len(bad_blob_hashes) > 0:
				result.affected_file_count = session.get_file_count_by_blob_hashes(bad_blob_hashes)
				result.affected_file_samples = [FileInfo.of(file) for file in session.get_file_by_blob_hashes(bad_blob_hashes, limit=1000)]
				result.affected_backup_ids = session.get_backup_ids_by_blob_hashes(bad_blob_hashes)

		self.logger.info('Blob validation done: total {}, validated {}, ok {}, bad {}'.format(
			result.total, result.validated, result.ok, len(bad_blob_hashes),
		))
		return result
