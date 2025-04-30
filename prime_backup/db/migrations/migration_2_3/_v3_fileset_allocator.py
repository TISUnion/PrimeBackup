import collections
import dataclasses
import enum
from typing import List, Optional, Dict, Tuple, Iterable

from prime_backup import logger
from prime_backup.db.migrations.migration_2_3._v3_session import _V3DbSession
from prime_backup.utils import collection_utils
from prime_backup.utils.lru_dict import LruDict


class _V3FileRole(enum.IntEnum):
	unknown = 0
	standalone = 1
	delta_override = 2
	delta_add = 3
	delta_remove = 4


def _sum_file_sizes(files: Iterable[dict]) -> Tuple[int, int]:
	file_raw_size_sum = 0
	file_stored_size_sum = 0

	for f in files:
		file_raw_size_sum += f['blob_raw_size'] or 0
		file_stored_size_sum += f['blob_stored_size'] or 0

	return file_raw_size_sum, file_stored_size_sum


@dataclasses.dataclass(frozen=True)
class _FilesetAllocateArgs:
	candidate_select_count: int = 2
	candidate_max_changes_ratio: float = 0.2
	max_delta_ratio: float = 1.5
	max_base_reuse_count: int = 100


@dataclasses.dataclass(frozen=True)
class _FilesetAllocateResult:
	fileset_base: dict
	fileset_delta: dict
	new_file_object_count: int


class _V3FilesetAllocator:
	FilesetFileCache = LruDict[int, List[dict]]

	def __init__(self, session: _V3DbSession, files: List[dict]):
		self.logger = logger.get()
		self.session = session
		self.files = files
		self.__fileset_files_cache: Optional[_V3FilesetAllocator.FilesetFileCache] = None

	@classmethod
	def __get_file_by_path(cls, files: List[dict]) -> Dict[str, dict]:
		return {f['path']: f for f in files}

	@classmethod
	def __are_files_content_equaled(cls, a: dict, b: dict) -> bool:
		return (
			a['path'] == b['path'] and a['mode'] == b['mode'] and
			a['content'] == b['content'] and a['blob_hash'] == b['blob_hash'] and
			a['uid'] == b['uid'] and a['gid'] == b['gid'] and
			a['mtime'] == b['mtime']
		)

	@classmethod
	def __calc_delta(cls, old: Dict[str, dict], new: Dict[str, dict]) -> collection_utils.DictValueDelta[dict]:
		return collection_utils.compute_dict_value_delta(old, new, cmp=cls.__are_files_content_equaled)

	def enable_fileset_files_cache(self, cache: FilesetFileCache):
		self.__fileset_files_cache = cache

	def __get_fileset_files(self, fileset_id: int) -> List[dict]:
		if self.__fileset_files_cache is not None:
			if (files := self.__fileset_files_cache.get(fileset_id, None)) is not None:
				return files

		files = self.session.v3_get_fileset_files(fileset_id)
		if self.__fileset_files_cache is not None:
			self.__fileset_files_cache.set(fileset_id, files)
		return files

	def allocate(self) -> _FilesetAllocateResult:
		@dataclasses.dataclass(frozen=True)
		class Candidate:
			fileset: dict
			file_by_path: Dict[str, dict]
			delta: collection_utils.DictValueDelta[dict]
			delta_size: int

		args = _FilesetAllocateArgs()
		c: Optional[Candidate] = None
		file_by_path = self.__get_file_by_path(self.files)

		for c_fileset in self.session.v3_get_last_n_base_fileset(limit=args.candidate_select_count):
			c_fileset_files = self.__get_fileset_files(c_fileset['id'])
			c_file_by_path = self.__get_file_by_path(c_fileset_files)
			delta = self.__calc_delta(old=c_file_by_path, new=file_by_path)
			self.logger.debug('Selecting fileset base candidate: id={} delta_size={}'.format(c_fileset['id'], delta.size()))
			if c_fileset['id'] <= 0 or c_fileset['base_id'] < 0:
				# should never happen, but just in case
				self.logger.error('Skipping corrupt fileset with id {}. Please validate the healthiness of the database'.format(c_fileset['id']))
				continue
			if delta.size() < len(file_by_path) * args.candidate_max_changes_ratio and (c is None or delta.size() < c.delta_size):
				c = Candidate(c_fileset, c_file_by_path, delta, delta.size())

		if c is not None:
			ref_cnt = self.session.v3_get_fileset_associated_backup_count(c.fileset['id'])
			delta_file_object_count_sum = self.session.v3_get_fileset_delta_file_object_count_sum(c.fileset['id'])
			delta_ratio = delta_file_object_count_sum / c.fileset['file_object_count'] if c.fileset['file_object_count'] > 0 else 0
			self.logger.debug('Fileset base candidate selected, id {}, ref_cnt {}, delta_file_object_count_sum {} (r={:.2f})'.format(
				c.fileset['id'], ref_cnt, delta_file_object_count_sum, delta_ratio,
			))

			if c is not None and ref_cnt >= args.max_base_reuse_count:
				self.logger.debug('Fileset base candidate {} has its ref_cnt {} >= {}, create a new fileset'.format(
					c.fileset['id'], ref_cnt, args.max_base_reuse_count
				))
				c = None
			if c is not None and delta_ratio >= args.max_delta_ratio:
				self.logger.info('Fileset base candidate {} has its delta_ratio {:.2f} >= {:.2f}, create a new fileset'.format(
					c.fileset['id'], delta_ratio, args.max_delta_ratio
				))
				c = None
		else:
			self.logger.debug('FilesetAllocator base fileset not found')

		if c is None:
			rss, sss = _sum_file_sizes(self.files)
			fileset_base = self.session.v3_insert('fileset', dict(
				base_id=0,
				file_object_count=len(self.files),
				file_count=len(self.files),
				file_raw_size_sum=rss,
				file_stored_size_sum=sss,
			), need_result=True)
			fileset_delta = self.session.v3_insert('fileset', dict(
				base_id=fileset_base['id'],
				file_object_count=0,
				file_count=0,
				file_raw_size_sum=0,
				file_stored_size_sum=0,
			), need_result=True)

			for file in self.files:
				file['fileset_id'] = fileset_base['id']
				file['role'] = _V3FileRole.standalone.value
				self.session.v3_insert('file', file)

			self.logger.debug('Created base fileset {}, len(files)={}'.format(fileset_base, len(self.files)))
			self.logger.debug('Created empty delta fileset {}'.format(fileset_delta))
			return _FilesetAllocateResult(fileset_base, fileset_delta, new_file_object_count=len(self.files))
		else:
			# reuse the existing base fileset
			delta_files: List[dict] = []

			# these sum are deltas
			file_count = 0
			file_raw_size_sum = 0
			file_stored_size_sum = 0

			# NOTES: We can only manipulate new files (which are references from self.files),
			#        DO NOT manipulate old files (which are files in existing fileset)
			for new_file in c.delta.added:
				new_file['role'] = _V3FileRole.delta_add.value
				file_count += 1
				file_raw_size_sum += (new_file['blob_raw_size'] or 0)
				file_stored_size_sum += (new_file['blob_stored_size'] or 0)
				delta_files.append(new_file)

			for old_new in c.delta.changed:
				old_new.new['role'] = _V3FileRole.delta_override.value
				file_raw_size_sum += (old_new.new['blob_raw_size'] or 0) - (old_new.old['blob_raw_size'] or 0)
				file_stored_size_sum += (old_new.new['blob_stored_size'] or 0) - (old_new.old['blob_stored_size'] or 0)
				delta_files.append(old_new.new)

			for old_file in c.delta.removed:
				file = dict(
					path=old_file['path'],
					role=_V3FileRole.delta_remove.value,
					mode=0,
					content=None,
					uid=None,
					gid=None,
					mtime=None,
				)
				file_count -= 1
				file_raw_size_sum -= (old_file['blob_raw_size'] or 0)
				file_stored_size_sum -= (old_file['blob_stored_size'] or 0)
				delta_files.append(file)

			fileset_delta = self.session.v3_insert('fileset', dict(
				base_id=c.fileset['id'],
				file_object_count=len(delta_files),
				file_count=file_count,
				file_raw_size_sum=file_raw_size_sum,
				file_stored_size_sum=file_stored_size_sum,
			), need_result=True)

			role_counter: Dict[_V3FileRole, int] = collections.defaultdict(int)
			for file in delta_files:
				file['fileset_id'] = fileset_delta['id']
				role_counter[_V3FileRole(file['role'])] += 1
				self.session.v3_insert('file', file)

			self.logger.debug('Created delta fileset {}, len(delta_files)={}, role counts={}'.format(
				fileset_delta, len(delta_files), {k.name: v for k, v in role_counter.items()},
			))
			return _FilesetAllocateResult(c.fileset, fileset_delta, new_file_object_count=len(delta_files))
