import dataclasses
import enum
import os
from pathlib import Path
from typing import Generator, Optional

from prime_backup.action.helpers.blob_creator_common import BqmReq, BqmRsp, BlobCreateContext, BlobCreatorBase, _FailureFileDeleter
from prime_backup.action.helpers.blob_pre_calc_result import BlobPrecalculateResult
from prime_backup.action.helpers.create_backup_utils import CreateBackupTimeCostKey, SourceFileNotFoundWrapper
from prime_backup.compressors import Compressor, CompressMethod
from prime_backup.db import schema
from prime_backup.db.values import BlobStorageMethod
from prime_backup.utils import blob_utils, file_utils, hash_utils, misc_utils
from prime_backup.utils.hash_utils import SizeAndHash


class _DirectBlobCreatePolicy(enum.Enum):
	"""
	the policy of how to create a blob from a given file path
	"""
	read_all = enum.auto()   # small files: read all in memory, calc hash                                |  read 1x, write 1x
	hash_once = enum.auto()  # files with unique size: compress+hash to temp file, then move             |  read 1x, write 1x, move 1x
	copy_hash = enum.auto()  # files that keep changing: copy to temp file, calc hash, compress to blob  |  read 2x, write 2x. need more spaces
	default = enum.auto()    # default policy: hash and check, then compress+hash to blob store          |  read 2x, write 1x


_READ_ALL_SIZE_THRESHOLD = 8 * 1024  # 8KiB
_HASH_ONCE_SIZE_THRESHOLD = 10 * 1024 * 1024  # 10MiB


@dataclasses.dataclass
class _DirectBlobPlan:
	policy: _DirectBlobCreatePolicy
	compress_method: CompressMethod
	can_copy_on_write: bool
	blob_hash: Optional[str]
	blob_content: Optional[bytes]


@dataclasses.dataclass
class _DirectBlobArtifact:
	blob_hash: str
	raw_size: int
	stored_size: int


@dataclasses.dataclass
class _DirectBlobCreateResult:
	existing_blob: Optional[schema.Blob] = None
	artifact: Optional[_DirectBlobArtifact] = None

	@classmethod
	def existing(cls, blob: schema.Blob) -> '_DirectBlobCreateResult':
		return cls(existing_blob=blob)

	@classmethod
	def created(cls, blob_hash: str, raw_size: int, stored_size: int) -> '_DirectBlobCreateResult':
		return cls(artifact=_DirectBlobArtifact(blob_hash, raw_size, stored_size))


class DirectBlobCreator(BlobCreatorBase):
	@dataclasses.dataclass(frozen=True)
	class Args:
		src_path: Path
		src_path_md5: str
		st: os.stat_result
		last_chance: bool
		is_mutating_file: bool

	def __init__(self, context: BlobCreateContext, args: Args):
		super().__init__(context)
		self.args = args

	def get_or_create(self) -> Generator[BqmReq, Optional[BqmRsp], schema.Blob]:
		plan = yield from self.__select_plan()

		if plan.blob_hash is not None:
			misc_utils.assert_true(plan.policy != _DirectBlobCreatePolicy.hash_once, 'unexpected policy')
			if (cache := (yield from self.query_cached_blob(plan.blob_hash))) is not None:
				return cache

		create_result = yield from self.__create_blob_artifact(plan)
		if create_result.existing_blob is not None:
			return create_result.existing_blob
		if (artifact := create_result.artifact) is None:
			raise AssertionError()

		self.ctx.blob_recorder.add_remove_file_rollbacker(blob_utils.get_blob_path(artifact.blob_hash))
		return self.ctx.blob_recorder.create_blob(
			self.ctx.session,
			storage_method=BlobStorageMethod.direct.value,
			hash=artifact.blob_hash,
			compress=plan.compress_method.name,
			raw_size=artifact.raw_size,
			stored_size=artifact.stored_size,
		)

	def __select_plan(self) -> Generator[BqmReq, Optional[BqmRsp], _DirectBlobPlan]:
		compress_method: CompressMethod = self.config.backup.get_compress_method_from_size(self.args.st.st_size)
		can_copy_on_write = self.__can_copy_on_write(self.args.st, compress_method)

		policy: Optional[_DirectBlobCreatePolicy] = None
		blob_hash: Optional[str] = None
		blob_content: Optional[bytes] = None
		pre_calc_blob_hash = self.__get_pre_calc_blob_hash()

		if self.args.last_chance or self.args.is_mutating_file:
			policy = _DirectBlobCreatePolicy.copy_hash
		elif pre_calc_blob_hash is not None:  # hash already calculated? just use default
			policy = _DirectBlobCreatePolicy.default
			blob_hash = pre_calc_blob_hash
		elif not can_copy_on_write:  # do tricks iff. no COW copy
			if self.args.st.st_size <= _READ_ALL_SIZE_THRESHOLD:
				policy = _DirectBlobCreatePolicy.read_all
				blob_content = self.__read_small_file()
				blob_hash = hash_utils.calc_bytes_hash(blob_content)
			elif self.args.st.st_size > _HASH_ONCE_SIZE_THRESHOLD:
				can_hash_once = not (yield from self.query_blob_size_exists(self.args.st.st_size))
				if can_hash_once:
					# it's certain that this blob is unique, but notes: the following code
					# cannot be interrupted (yield), or another generator could make a same blob
					policy = _DirectBlobCreatePolicy.hash_once

		if policy is None:
			policy = _DirectBlobCreatePolicy.default
			with self.ctx.time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_io_read):
				with SourceFileNotFoundWrapper.wrap(self.args.src_path):
					blob_hash = hash_utils.calc_file_hash(self.args.src_path)

		return _DirectBlobPlan(
			policy=policy,
			compress_method=compress_method,
			can_copy_on_write=can_copy_on_write,
			blob_hash=blob_hash,
			blob_content=blob_content,
		)

	def __can_copy_on_write(self, st: os.stat_result, compress_method: CompressMethod) -> bool:
		return (
				file_utils.HAS_COPY_FILE_RANGE and
				compress_method == CompressMethod.plain and
				self.ctx.blob_store_in_cow_fs is True and
				self.ctx.blob_store_st is not None and st.st_dev == self.ctx.blob_store_st.st_dev
		)

	def __get_pre_calc_blob_hash(self) -> Optional[str]:
		pre_cal_result: Optional[BlobPrecalculateResult] = self.ctx.pre_calc_result_getter(self.args.src_path)
		if pre_cal_result is not None and (self.args.st.st_size != pre_cal_result.size or pre_cal_result.should_be_chunked is True):
			self.logger.debug('Drop pre cal result for path {} due to stat mismatched, st.st_size {}, pre_cal_result {}'.format(
				self.args.src_path, self.args.st.st_size, pre_cal_result.simple_repr(),
			))
			pre_cal_result = None
		return pre_cal_result.hash if pre_cal_result is not None else None

	def __read_small_file(self) -> bytes:
		with self.ctx.time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_io_read):
			with SourceFileNotFoundWrapper.open_rb(self.args.src_path, 'rb') as f:
				blob_content = f.read(_READ_ALL_SIZE_THRESHOLD + 1)
		if len(blob_content) > _READ_ALL_SIZE_THRESHOLD:
			self.log_and_raise_blob_file_changed('Read too many bytes for read_all policy, stat: {}, read: {}'.format(self.args.st.st_size, len(blob_content)), self.args.last_chance)
		return blob_content

	def __create_blob_artifact(self, plan: _DirectBlobPlan) -> Generator[BqmReq, Optional[BqmRsp], _DirectBlobCreateResult]:
		compressor = Compressor.create(plan.compress_method)
		if plan.policy == _DirectBlobCreatePolicy.copy_hash:
			return (yield from self.__create_by_copy_hash(compressor))
		if plan.policy == _DirectBlobCreatePolicy.hash_once:
			return self.__create_by_hash_once(compressor, plan)
		if plan.policy in (_DirectBlobCreatePolicy.read_all, _DirectBlobCreatePolicy.default):
			return self.__create_by_prehashed_content(compressor, plan)
		raise AssertionError('bad policy {!r}'.format(plan.policy))

	def __create_by_copy_hash(self, compressor: Compressor) -> Generator[BqmReq, Optional[BqmRsp], _DirectBlobCreateResult]:
		# copy to temp file, calc hash, then compress to blob store
		with self.ctx.make_temp_file(self.args.src_path_md5) as temp_file_path, _FailureFileDeleter() as file_deleter:
			with self.ctx.time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_io_copy), SourceFileNotFoundWrapper.wrap(self.args.src_path):
				file_utils.copy_file_fast(self.args.src_path, temp_file_path)
			with self.ctx.time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_io_read):
				blob_hash = hash_utils.calc_file_hash(temp_file_path)

			misc_utils.assert_true(self.args.last_chance or self.args.is_mutating_file, 'only last_chance=True or is_mutating_file=True is allowed for the copy_hash policy')
			if (cache := (yield from self.query_cached_blob(blob_hash))) is not None:
				return _DirectBlobCreateResult.existing(cache)

			blob_path = blob_utils.get_blob_path(blob_hash)
			file_deleter.mark(blob_path)
			with self.ctx.time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_io_copy):
				cr = compressor.copy_compressed(temp_file_path, blob_path, calc_hash=False, estimate_read_size=self.args.st.st_size)
			return _DirectBlobCreateResult.created(blob_hash, cr.read_size, cr.write_size)

	def __create_by_hash_once(self, compressor: Compressor, plan: _DirectBlobPlan) -> _DirectBlobCreateResult:
		# read once, compress+hash to temp file, then move
		misc_utils.assert_true(plan.blob_hash is None, 'blob_hash should not be calculated')
		with self.ctx.make_temp_file(self.args.src_path_md5) as temp_file_path, _FailureFileDeleter() as file_deleter:
			with self.ctx.time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_io_copy):
				cr = compressor.copy_compressed(self.args.src_path, temp_file_path, calc_hash=True, estimate_read_size=self.args.st.st_size, open_r_func=SourceFileNotFoundWrapper.open_rb)
			self.__check_changes(None, cr.read_size, None)  # the size must be unchanged to satisfy the uniqueness

			blob_hash = misc_utils.ensure_type(cr.read_hash, str)
			blob_path = blob_utils.get_blob_path(blob_hash)
			file_deleter.mark(blob_path)

			# reference: shutil.move, but os.replace is used
			try:
				with self.ctx.time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_fs):
					os.replace(temp_file_path, blob_path)
			except OSError:
				# The temp dir is in the different file system to the blob store?
				# Whatever, use file copy as the fallback
				# the temp file will be deleted automatically
				with self.ctx.time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_io_copy):
					file_utils.copy_file_fast(temp_file_path, blob_path)
			return _DirectBlobCreateResult.created(blob_hash, cr.read_size, cr.write_size)

	def __create_by_prehashed_content(self, compressor: Compressor, plan: _DirectBlobPlan) -> _DirectBlobCreateResult:
		misc_utils.assert_true(plan.blob_hash is not None, 'blob_hash is None')
		blob_hash = misc_utils.ensure_type(plan.blob_hash, str)
		blob_path = blob_utils.get_blob_path(blob_hash)

		with _FailureFileDeleter() as file_deleter:
			file_deleter.mark(blob_path)

			if plan.policy == _DirectBlobCreatePolicy.read_all:
				misc_utils.assert_true(plan.blob_content is not None, 'blob_content is None')
				blob_content = misc_utils.ensure_type(plan.blob_content, bytes)
				return self.__write_read_all_blob(blob_hash, blob_path, compressor, blob_content)
			if plan.policy == _DirectBlobCreatePolicy.default:
				return self.__write_default_blob(blob_hash, blob_path, compressor, plan)
			raise AssertionError('bad policy {!r}'.format(plan.policy))

	def __write_read_all_blob(self, blob_hash: str, blob_path: Path, compressor: Compressor, blob_content: bytes) -> _DirectBlobCreateResult:
		with self.ctx.time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_io_write):
			with compressor.open_compressed_bypassed(blob_path) as (writer, f):
				f.write(blob_content)
		return _DirectBlobCreateResult.created(blob_hash, len(blob_content), writer.get_write_len())

	def __write_default_blob(self, blob_hash: str, blob_path: Path, compressor: Compressor, plan: _DirectBlobPlan) -> _DirectBlobCreateResult:
		if plan.can_copy_on_write and plan.compress_method == CompressMethod.plain:
			# fast copy, then calc size and hash to verify
			with self.ctx.time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_io_copy), SourceFileNotFoundWrapper.wrap(self.args.src_path):
				file_utils.copy_file_fast(self.args.src_path, blob_path)
			with self.ctx.time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_io_read):
				actual_sah = hash_utils.calc_file_size_and_hash(blob_path)
			raw_size = stored_size = actual_sah.size
		else:
			# copy+compress+hash to blob store
			with self.ctx.time_costs.measure_time_cost(CreateBackupTimeCostKey.kind_io_copy):
				cr = compressor.copy_compressed(self.args.src_path, blob_path, calc_hash=True, estimate_read_size=self.args.st.st_size, open_r_func=SourceFileNotFoundWrapper.open_rb)
			raw_size, stored_size = cr.read_size, cr.write_size
			actual_sah = SizeAndHash(cr.read_size, misc_utils.ensure_type(cr.read_hash, str))
		self.__check_changes(blob_hash, actual_sah.size, actual_sah.hash)
		return _DirectBlobCreateResult.created(blob_hash, raw_size, stored_size)

	def __check_changes(self, blob_hash: Optional[str], new_size: int, new_hash: Optional[str]):
		if new_size != self.args.st.st_size:
			self.log_and_raise_blob_file_changed('Blob size mismatch, previous: {}, current: {}'.format(self.args.st.st_size, new_size), self.args.last_chance)
		if blob_hash is not None and new_hash is not None and new_hash != blob_hash:
			self.log_and_raise_blob_file_changed('Blob hash mismatch, previous: {}, current: {}'.format(blob_hash, new_hash), self.args.last_chance)
