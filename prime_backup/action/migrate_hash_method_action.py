import dataclasses
import shutil
import time
from pathlib import Path
from typing import Callable, Dict, Generic, List, Protocol, TypeVar, cast, Tuple

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.action.helpers.chunk_grouper import ChunkGrouper
from prime_backup.action.helpers.chunk_io import ChunkIO
from prime_backup.compressors import Compressor
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.db.values import BlobStorageMethod
from prime_backup.exceptions import PrimeBackupError
from prime_backup.types.chunk_info import ChunkInfo
from prime_backup.types.hash_method import HashMethod
from prime_backup.utils import blob_utils, hash_utils

_READ_BUF_SIZE = 128 * 1024
_FILE_BLOB_HASH_BATCH_SIZE = 200
_T = TypeVar('_T')


class HashCollisionError(PrimeBackupError):
	"""
	Same hash value, between 2 hash methods
	"""
	pass


class _HashObject(Protocol):
	id: int
	hash: str


@dataclasses.dataclass(frozen=True)
class _HashMove(Generic[_T]):
	object: _T
	old_hash: str
	new_hash: str
	has_file_to_move: bool = False

	@property
	def id(self) -> int:
		return cast(_HashObject, self.object).id

	@property
	def changed(self) -> bool:
		return self.old_hash != self.new_hash


class _MoveJournal:
	def __init__(self):
		self.__moves: List[Tuple[Path, Path]] = []

	def move(self, src: Path, dst: Path):
		if dst.exists():
			raise FileExistsError(dst)
		shutil.move(src, dst)
		self.__moves.append((src, dst))

	def rollback(self):
		for src, dst in reversed(self.__moves):
			shutil.move(dst, src)

	def clear(self):
		self.__moves.clear()


def _changed_moves(moves: List[_HashMove[_T]]) -> List[_HashMove[_T]]:
	return [move for move in moves if move.changed]


class MigrateHashMethodAction(Action[None]):
	def __init__(self, new_hash_method: HashMethod):
		super().__init__()
		self.new_hash_method = new_hash_method
		self.__move_journal = _MoveJournal()

	# ==================== Checks ====================

	@classmethod
	def __ensure_hashes_can_migrate(cls, moves: List[_HashMove], object_name: str):
		old_hash_ids: Dict[str, int] = {}
		new_hash_ids: Dict[str, int] = {}
		for move in moves:
			old_hash_ids[move.old_hash] = move.id
			if (other_id := new_hash_ids.get(move.new_hash)) is not None:
				raise HashCollisionError('{} hash collision: {}, object ids {} and {}'.format(object_name, move.new_hash, other_id, move.id))
			new_hash_ids[move.new_hash] = move.id

		for move in moves:
			if not move.changed:
				continue
			if (old_owner_id := old_hash_ids.get(move.new_hash)) is not None and old_owner_id != move.id:
				raise HashCollisionError('{} hash conflicts with existing old hash: {}, object ids {} and {}'.format(object_name, move.new_hash, old_owner_id, move.id))

	@classmethod
	def __ensure_paths_can_migrate(cls, moves: List[_HashMove], get_path: Callable[[str], Path]):
		for move in moves:
			if move.changed and move.has_file_to_move and (new_hash_path := get_path(move.new_hash)).exists():
				raise FileExistsError(new_hash_path)

	# ==================== Hash Calculation ====================

	def __calc_direct_blob_new_hash(self, blob: schema.Blob) -> str:
		with Compressor.create(blob.compress).open_decompressed(blob_utils.get_blob_path(blob.hash)) as f:
			sah = hash_utils.calc_reader_size_and_hash(f, hash_method=self.new_hash_method)
		if sah.size != blob.raw_size:
			raise ValueError('raw size mismatch for blob {}, expect {}, found {}'.format(blob.hash, blob.raw_size, sah.size))
		return sah.hash

	def __calc_chunked_blob_new_hash(self, session: DbSession, blob: schema.Blob) -> str:
		hasher = self.new_hash_method.value.create_hasher()
		size = 0
		for offset_chunk in session.get_blob_chunks(blob.id):
			chunk = offset_chunk.chunk
			chunk_size = 0
			with ChunkIO(ChunkInfo.of(chunk)).open_decompressed() as f:
				while True:
					buf = f.read(_READ_BUF_SIZE)
					if len(buf) == 0:
						break
					hasher.update(buf)
					chunk_size += len(buf)
			if chunk_size != chunk.raw_size:
				raise ValueError('raw size mismatch for chunk {}, expect {}, found {}'.format(chunk.hash, chunk.raw_size, chunk_size))
			size += chunk_size
		if size != blob.raw_size:
			raise ValueError('raw size mismatch for chunked blob {}, expect {}, found {}'.format(blob.hash, blob.raw_size, size))
		return hasher.hexdigest()

	def __calc_chunk_new_hash(self, chunk: schema.Chunk) -> str:
		with ChunkIO(ChunkInfo.of(chunk)).open_decompressed_bypassed() as (reader, f):
			sah = hash_utils.calc_reader_size_and_hash(f, hash_method=self.new_hash_method)
		if reader.get_read_len() != chunk.stored_size:
			raise ValueError('stored size mismatch for chunk {}, expect {}, found {}'.format(chunk.hash, chunk.stored_size, reader.get_read_len()))
		if sah.size != chunk.raw_size:
			raise ValueError('raw size mismatch for chunk {}, expect {}, found {}'.format(chunk.hash, chunk.raw_size, sah.size))
		return sah.hash

	# ==================== Move Collection ====================

	def __collect_blob_moves(self, session: DbSession) -> List[_HashMove[schema.Blob]]:
		moves: List[_HashMove[schema.Blob]] = []
		blobs = session.list_blobs()
		total = len(blobs)
		for i, blob in enumerate(blobs):
			if blob.storage_method == BlobStorageMethod.direct.value:
				new_hash = self.__calc_direct_blob_new_hash(blob)
				has_file_to_move = True
			elif blob.storage_method == BlobStorageMethod.chunked.value:
				new_hash = self.__calc_chunked_blob_new_hash(session, blob)
				has_file_to_move = False
			else:
				raise ValueError('unsupported blob storage method {}'.format(blob.storage_method))

			moves.append(_HashMove(object=blob, old_hash=blob.hash, new_hash=new_hash, has_file_to_move=has_file_to_move))
			if (i + 1) % 1000 == 0 or i + 1 == total:
				self.logger.info('Calculated blob hashes {} / {}'.format(i + 1, total))

		self.__ensure_hashes_can_migrate(moves, 'blob')
		self.__ensure_paths_can_migrate(moves, blob_utils.get_blob_path)
		return _changed_moves(moves)

	def __collect_chunk_moves(self, session: DbSession) -> List[_HashMove[schema.Chunk]]:
		moves: List[_HashMove[schema.Chunk]] = []
		chunks = session.list_chunks()
		total = len(chunks)
		for i, chunk in enumerate(chunks):
			new_hash = self.__calc_chunk_new_hash(chunk)
			moves.append(_HashMove(object=chunk, old_hash=chunk.hash, new_hash=new_hash, has_file_to_move=False))
			if (i + 1) % 2000 == 0 or i + 1 == total:
				self.logger.info('Calculated chunk hashes {} / {}'.format(i + 1, total))

		self.__ensure_hashes_can_migrate(moves, 'chunk')
		return _changed_moves(moves)

	def __regroup_chunked_blobs(self, session: DbSession):
		# Step 1 - collect blob -> ordered chunks before destroying the binding chain
		chunked_blobs = session.list_blobs_by_storage_method(BlobStorageMethod.chunked)
		total = len(chunked_blobs)
		blob_chunks_map: Dict[int, Dict[int, schema.Chunk]] = {}
		for blob in chunked_blobs:
			offset_chunks = session.get_blob_chunks(blob.id)  # sorted by absolute_offset
			blob_chunks_map[blob.id] = {oc.offset: oc.chunk for oc in offset_chunks}

		chunk_group_count = session.get_chunk_group_count()
		self.logger.info('Dropping {} chunk group and all bindings, then re-grouping {} chunked blob with the new chunk hashes'.format(chunk_group_count, total))

		# Step 2 - wipe all chunk group data
		session.delete_all_blob_chunk_group_bindings()
		session.delete_all_chunk_group_chunk_bindings()
		session.delete_all_chunk_groups()
		session.flush()

		# Step 3 - re-group using ChunkGrouper (which applies the endswith('00') cut rule)
		if total == 0:
			return
		chunk_grouper = ChunkGrouper(session, None)
		for i, blob in enumerate(chunked_blobs):
			chunk_grouper.create_chunk_groups(blob, blob_chunks_map[blob.id])
			if (i + 1) % 200 == 0 or i + 1 == total:
				self.logger.info('Re-grouped chunked blobs {} / {}'.format(i + 1, total))

	# ==================== DB Updates ====================

	def __update_file_blob_hashes(self, session: DbSession, moves: List[_HashMove[schema.Blob]]):
		hash_mapping = {move.old_hash: move.new_hash for move in moves}
		if len(hash_mapping) == 0:
			return
		for file in session.get_file_by_blob_hashes(list(hash_mapping.keys())):
			if file.blob_hash is None:
				raise AssertionError('file {!r} has no blob_hash'.format(file))
			file.blob_hash = hash_mapping[file.blob_hash]

	# ==================== Migration Steps ====================

	def __move_files_to_new_hashes(self, object_name: str, moves: List[_HashMove], get_path: Callable[[str], Path]):
		total = sum(1 for move in moves if move.has_file_to_move)
		done = 0
		for move in moves:
			if move.has_file_to_move:
				self.__move_journal.move(get_path(move.old_hash), get_path(move.new_hash))
				done += 1
				if done % 2000 == 0 or done == total:
					self.logger.info('Moved {} files {} / {}'.format(object_name, done, total))

	def __migrate_blob_hashes(self, session: DbSession, moves: List[_HashMove[schema.Blob]]):
		self.__move_files_to_new_hashes('blob', moves, blob_utils.get_blob_path)
		for offset in range(0, len(moves), _FILE_BLOB_HASH_BATCH_SIZE):
			batch = moves[offset:offset + _FILE_BLOB_HASH_BATCH_SIZE]
			with session.no_auto_flush():
				for move in batch:
					move.object.hash = move.new_hash
				self.__update_file_blob_hashes(session, batch)
			session.flush()

	def __migrate_chunk_hashes(self, session: DbSession, moves: List[_HashMove[schema.Chunk]]):
		for move in moves:
			move.object.hash = move.new_hash
		session.flush()

	def __rollback_files(self):
		self.__move_journal.rollback()

	# ==================== Entry Point ====================

	@override
	def run(self) -> None:
		t = time.time()
		db_committed = False
		try:
			with DbAccess.open_session() as session:
				meta = session.get_db_meta()
				if meta.hash_method == self.new_hash_method.name:
					self.logger.info('Hash method of the database is already {}, no need to migrate'.format(self.new_hash_method.name))
					return

				self.logger.info('Migrating hash method from {} to {}'.format(meta.hash_method, self.new_hash_method.name))
				blob_utils.prepare_blob_directories()

				self.__migrate_blob_hashes(session, self.__collect_blob_moves(session))
				self.__migrate_chunk_hashes(session, self.__collect_chunk_moves(session))
				self.__regroup_chunked_blobs(session)

				meta = session.get_db_meta()
				meta.hash_method = self.new_hash_method.name

			db_committed = True
			try:
				self.logger.info('Syncing config and variables')
				DbAccess.sync_meta_cache()
				self.config.backup.hash_method = self.new_hash_method
				self.__move_journal.clear()
			except Exception:
				self.logger.fatal('DB committed but in-memory sync failed, plugin restart required')
				raise
			self.logger.info('Hash method migration done, cost {}s'.format(round(time.time() - t, 2)))

		except Exception:
			self.logger.warning('Error occurs during migration, applying rollback')
			if not db_committed:
				self.__rollback_files()
			raise
