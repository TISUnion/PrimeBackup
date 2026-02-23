import collections
import dataclasses
import enum
from typing import List, Dict, Set

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.compressors import Compressor, CompressMethod
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.db.values import BlobStorageMethod
from prime_backup.types.blob_info import BlobInfo
from prime_backup.types.file_info import FileInfo
from prime_backup.utils import blob_utils, hash_utils, collection_utils
from prime_backup.utils.thread_pool import FailFastBlockingThreadPool


class BadBlobItemType(enum.Enum):
	invalid = enum.auto()  # wierd blobs
	missing = enum.auto()  # the file of the blob is missing
	corrupted = enum.auto()  # decompress failed
	mismatched = enum.auto()  # hash or size mismatch
	bad_layout = enum.auto()  # chunk group binding layout invalid
	orphan = enum.auto()  # orphan blobs


@dataclasses.dataclass(frozen=True)
class BadBlobItem:
	blob: BlobInfo
	typ: BadBlobItemType
	desc: str


@dataclasses.dataclass
class ValidateBlobsResult:
	total: int = 0
	validated: int = 0
	ok: int = 0
	bad_blobs: List[BadBlobItem] = dataclasses.field(default_factory=list)

	affected_file_count: int = 0
	affected_file_samples: List[FileInfo] = dataclasses.field(default_factory=list)
	affected_fileset_ids: List[int] = dataclasses.field(default_factory=list)
	affected_backup_ids: List[int] = dataclasses.field(default_factory=list)

	@property
	def bad(self) -> int:
		return self.validated - self.ok

	def add_bad(self, blob: BlobInfo, typ: BadBlobItemType, msg: str):
		self.bad_blobs.append(BadBlobItem(blob, typ, msg))

	def group_bad_by_type(self) -> Dict[BadBlobItemType, List[BadBlobItem]]:
		result: Dict[BadBlobItemType, List[BadBlobItem]] = {}
		for bad_blob in self.bad_blobs:
			result.setdefault(bad_blob.typ, []).append(bad_blob)
		return result


@dataclasses.dataclass(frozen=True)
class _SubResultStore:
	results: List[ValidateBlobsResult] = dataclasses.field(default_factory=list)

	def acquire(self) -> ValidateBlobsResult:
		sr = ValidateBlobsResult()
		self.results.append(sr)
		return sr


class ValidateBlobsAction(Action[ValidateBlobsResult]):
	@override
	def is_interruptable(self) -> bool:
		return True

	def __validate_direct_blobs(self, result: ValidateBlobsResult, blobs: List[BlobInfo]) -> Set[str]:
		if len(blobs) == 0:
			return set()

		good_blob_hashes = set()

		def validate_one_blob(blob: BlobInfo):
			if blob.storage_method != BlobStorageMethod.direct:
				raise AssertionError()

			blob_path = blob_utils.get_blob_path(blob.hash)

			if not blob_path.is_file():
				result.add_bad(blob, BadBlobItemType.missing, f'blob file {blob_path} does not exist')
				return

			try:
				# Notes: There are some codes that use `CompressMethod[blob.compress]`,
				# which might fail hard if the blob.compress is invalid.
				# Maybe we need to make them fail-proof somehow?
				compressor = Compressor.create(blob.compress)
			except ValueError:
				result.add_bad(blob, BadBlobItemType.invalid, f'unknown compress method {blob.compress!r}')
				return

			try:
				with compressor.open_decompressed_bypassed(blob_path) as (reader, f_decompressed):
					sah = hash_utils.calc_reader_size_and_hash(f_decompressed)
			except Exception as e:
				result.add_bad(blob, BadBlobItemType.corrupted, f'cannot read and decompress blob file: ({type(e)} {e})')
				return

			file_size = reader.get_read_len()
			if file_size != blob.stored_size:
				result.add_bad(blob, BadBlobItemType.mismatched, f'stored size mismatch, expect {blob.stored_size}, found {file_size}')
				return
			if sah.hash != blob.hash:
				result.add_bad(blob, BadBlobItemType.mismatched, f'hash mismatch, expect {blob.hash}, found {sah.hash}')
				return
			if sah.size != blob.raw_size:
				result.add_bad(blob, BadBlobItemType.mismatched, f'raw size mismatch, expect {blob.raw_size}, found {sah.size}')
				return

			good_blob_hashes.add(blob.hash)

		with FailFastBlockingThreadPool('validator') as pool:
			for b in blobs:
				if self.is_interrupted.is_set():
					break
				pool.submit(validate_one_blob, b)

		return good_blob_hashes

	def __validate_chunked_blobs(self, session: DbSession, result: ValidateBlobsResult, blobs: List[BlobInfo]) -> Set[str]:
		if len(blobs) == 0 or self.is_interrupted.is_set():
			return set()

		all_blob_ids = [blob.id for blob in blobs]
		all_bindings = session.get_blob_chunk_group_bindings_for_blobs(all_blob_ids)
		all_bindings_by_blob_id: Dict[int, List[schema.BlobChunkGroupBinding]] = collections.defaultdict(list)
		for bd in all_bindings:
			all_bindings_by_blob_id[bd.blob_id].append(bd)
		for bd_lst in all_bindings_by_blob_id.values():
			def blob_binding_key_getter(b_: schema.BlobChunkGroupBinding):
				return b_.chunk_group_offset

			bd_lst.sort(key=blob_binding_key_getter)
		all_chunk_groups_by_id = session.get_chunk_groups_by_ids([bd.chunk_group_id for bd in all_bindings])

		def validate_one_blob(blob: BlobInfo):
			if blob.storage_method != BlobStorageMethod.chunked:
				raise AssertionError()
			if blob.compress != CompressMethod.plain:
				result.add_bad(blob, BadBlobItemType.corrupted, f'the compress field of chunked blob should always be plain, found {blob.compress}')
				return

			# NOTES: validations for the chunks are done in ValidateChunksAction
			good_blob_hashes.add(blob.hash)

			group_bindings = all_bindings_by_blob_id.get(blob.id, [])
			if len(group_bindings) == 0:
				result.add_bad(blob, BadBlobItemType.invalid, f'chunked blob with 0 chunk group binding')
				return

			raw_size_sum = 0
			stored_size_sum = 0
			offset = 0
			for binding in group_bindings:
				if offset != binding.chunk_group_offset:
					result.add_bad(blob, BadBlobItemType.bad_layout, f'chunk group binding offset mismatch, expect {offset}, actual {binding.chunk_group_offset}')
					return
				chunk_group = all_chunk_groups_by_id.get(binding.chunk_group_id)
				if chunk_group is None:
					result.add_bad(blob, BadBlobItemType.bad_layout, f'chunk group binding at offset {offset} refer to a not-exists chunk {binding.chunk_group_id}')
					return

				raw_size_sum += chunk_group.chunk_raw_size_sum
				stored_size_sum += chunk_group.chunk_stored_size_sum
				offset += chunk_group.chunk_raw_size_sum

			if raw_size_sum != blob.raw_size:
				result.add_bad(blob, BadBlobItemType.mismatched, f'raw size sum mismatch, expect {raw_size_sum}, found {blob.raw_size}')
				return
			if stored_size_sum != blob.stored_size:
				result.add_bad(blob, BadBlobItemType.mismatched, f'stored size sum mismatch, expect {stored_size_sum}, found {blob.stored_size}')
				return

		good_blob_hashes = set()
		for b in blobs:
			if self.is_interrupted.is_set():
				break
			validate_one_blob(b)
		return good_blob_hashes

	def __validate(self, session: DbSession, result: ValidateBlobsResult, blobs: List[BlobInfo]):
		blobs_by_storage_method: Dict[BlobStorageMethod, List[BlobInfo]] = collections.defaultdict(list)
		for blob in blobs:
			if not blob.id:
				result.add_bad(blob, BadBlobItemType.invalid, f'invalid id {blob.id!r}')
				continue
			if blob.storage_method == BlobStorageMethod.unknown:
				result.add_bad(blob, BadBlobItemType.invalid, 'unknown storage_method')
				continue
			blobs_by_storage_method[blob.storage_method].append(blob)

		sub_results = _SubResultStore()
		good_blob_hashes = set()
		good_blob_hashes |= self.__validate_direct_blobs(sub_results.acquire(), blobs_by_storage_method[BlobStorageMethod.direct])
		for chunked_blob_part in collection_utils.slicing_iterate(blobs_by_storage_method[BlobStorageMethod.chunked], 40):
			good_blob_hashes |= self.__validate_chunked_blobs(session, sub_results.acquire(), chunked_blob_part)
		if self.is_interrupted.is_set():
			return

		for sr in sub_results.results:
			result.validated += sr.validated
			result.ok += sr.ok
			result.bad_blobs.extend(sr.bad_blobs)

		remaining_good_blobs = [blob for blob in blobs if blob.hash in good_blob_hashes]
		orphan_hashes = set(session.filtered_orphan_blob_hashes([blob.hash for blob in remaining_good_blobs]))
		for blob in remaining_good_blobs:
			if blob.hash in orphan_hashes:
				result.add_bad(blob, BadBlobItemType.orphan, f'orphan blob with 0 associated file, hash {blob.hash}')
				good_blob_hashes.remove(blob.hash)

		result.validated += len(blobs)
		result.ok += len(good_blob_hashes)

	@override
	def run(self) -> ValidateBlobsResult:
		self.logger.info('Blob validation start')
		result = ValidateBlobsResult()

		session: DbSession
		with DbAccess.open_session() as session:
			result.total = session.get_blob_count()
			cnt = 0
			for blobs in session.iterate_blob_batch(batch_size=3000):
				if self.is_interrupted.is_set():
					break
				cnt += len(blobs)
				self.logger.info('Validating {} / {} blobs'.format(cnt, result.total))
				self.__validate(session, result, [BlobInfo.of(blob) for blob in blobs])

			bad_blob_hashes = [bbi.blob.hash for bbi in result.bad_blobs]
			if len(bad_blob_hashes) > 0:
				result.affected_file_count = session.get_file_count_by_blob_hashes(bad_blob_hashes)
				result.affected_file_samples = [FileInfo.of(file) for file in session.get_file_by_blob_hashes(bad_blob_hashes, limit=1000)]
				result.affected_fileset_ids = session.get_fileset_ids_by_blob_hashes(bad_blob_hashes)
				result.affected_backup_ids = session.get_backup_ids_by_fileset_ids(result.affected_fileset_ids)

		self.logger.info('Blob validation done: total {}, validated {}, ok {}, bad {}'.format(
			result.total, result.validated, result.ok, len(bad_blob_hashes),
		))
		return result
