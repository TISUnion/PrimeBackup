import dataclasses
from typing import List, NamedTuple, Dict

from prime_backup.action import Action
from prime_backup.compressors import Compressor
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.types.blob_info import BlobInfo
from prime_backup.utils import blob_utils, hash_utils


class BadBlobItem(NamedTuple):
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
	affected_backup_ids: List[int] = 0


class ValidateBlobsAction(Action[ValidateBlobsResult]):
	def is_interruptable(self) -> bool:
		return True

	def __validate(self, session: DbSession, result: ValidateBlobsResult, blobs: List[BlobInfo]):
		hash_to_blobs: Dict[str, BlobInfo] = {}  # store "good" blobs only
		for blob in blobs:
			if self.is_interrupted.is_set():
				break

			result.validated += 1
			blob_path = blob_utils.get_blob_path(blob.hash)

			if not blob_path.is_file():
				result.missing.append(BadBlobItem(blob, 'blob file does not exist'))
				continue

			try:
				compressor = Compressor.create(blob.compress)
			except ValueError:
				result.invalid.append(BadBlobItem(blob, f'unknown compress method {blob.compress!r}'))
				continue

			try:
				with compressor.open_decompressed_bypassed(blob_path) as (reader, f_decompressed):
					sah = hash_utils.calc_reader_size_and_hash(f_decompressed)
			except Exception as e:
				result.corrupted.append(BadBlobItem(blob, f'cannot read and decompress blob file: ({type(e)} {e}'))
				continue

			file_size = reader.get_read_len()
			if file_size != blob.stored_size:
				result.mismatched.append(BadBlobItem(blob, f'stored size mismatch, expect {blob.stored_size}, found {file_size}'))
				continue
			if sah.hash != blob.hash:
				result.mismatched.append(BadBlobItem(blob, f'hash mismatch, expect {blob.hash}, found {sah.hash}'))
				continue
			if sah.size != blob.raw_size:
				result.mismatched.append(BadBlobItem(blob, f'raw size mismatch, expect {blob.raw_size}, found {sah.size}'))
				continue

			hash_to_blobs[blob.hash] = blob

		orphan_hashes = set(session.filtered_orphan_blob_hashes(list(hash_to_blobs.keys())))
		for h, blob in hash_to_blobs.items():
			if h in orphan_hashes:
				result.orphan.append(BadBlobItem(blob, f'orphan blob with 0 associated file, hash {h}'))
			else:
				result.ok += 1

	def run(self) -> ValidateBlobsResult:
		result = ValidateBlobsResult()
		with DbAccess.open_session() as session:
			result.total = session.get_blob_count()
			limit, offset = 3000, 0
			while not self.is_interrupted.is_set():
				blobs = session.list_blobs(limit=limit, offset=offset)
				if len(blobs) == 0:
					break
				self.__validate(session, result, list(map(BlobInfo.of, blobs)))
				offset += limit

			bad_blob_hashes = []
			bad_blob_hashes.extend([bbi.blob.hash for bbi in result.invalid])
			bad_blob_hashes.extend([bbi.blob.hash for bbi in result.missing])
			bad_blob_hashes.extend([bbi.blob.hash for bbi in result.corrupted])
			bad_blob_hashes.extend([bbi.blob.hash for bbi in result.mismatched])
			bad_blob_hashes.extend([bbi.blob.hash for bbi in result.orphan])
			if len(bad_blob_hashes) > 0:
				result.affected_file_count = session.get_file_count_by_blob_hashes(bad_blob_hashes)
				result.affected_backup_ids = session.get_backup_ids_by_blob_hashes(bad_blob_hashes)

		return result
