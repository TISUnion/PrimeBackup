import contextlib
import dataclasses
import enum
from typing import List, Dict, Optional

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.compressors import Compressor
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.types.chunk_info import ChunkInfo
from prime_backup.utils import chunk_utils, hash_utils
from prime_backup.utils.thread_pool import FailFastBlockingThreadPool


class BadChunkItemType(enum.Enum):
	invalid = enum.auto()
	orphan = enum.auto()
	missing_file = enum.auto()
	corrupted = enum.auto()
	mismatched = enum.auto()


@dataclasses.dataclass(frozen=True)
class BadChunkItem:
	chunk: ChunkInfo
	typ: BadChunkItemType
	desc: str


@dataclasses.dataclass
class ValidateChunksResult:
	total: int = 0
	validated: int = 0
	ok: int = 0
	bad_chunks: List[BadChunkItem] = dataclasses.field(default_factory=list)

	@property
	def bad(self) -> int:
		return len(self.bad_chunks)

	def add_bad(self, chunk: ChunkInfo, typ: BadChunkItemType, msg: str):
		self.bad_chunks.append(BadChunkItem(chunk, typ, msg))

	def group_bad_by_type(self) -> Dict[BadChunkItemType, List[BadChunkItem]]:
		result: Dict[BadChunkItemType, List[BadChunkItem]] = {}
		for bad_chunk in self.bad_chunks:
			result.setdefault(bad_chunk.typ, []).append(bad_chunk)
		return result


class ValidateChunksAction(Action[ValidateChunksResult]):
	@override
	def is_interruptable(self) -> bool:
		return True

	def __validate(self, session: DbSession, result: ValidateChunksResult, chunks: List[ChunkInfo]):
		id_to_good_chunks: Dict[int, ChunkInfo] = {}

		def validate_one_chunk(chunk: ChunkInfo):
			if not chunk.id:
				result.add_bad(chunk, BadChunkItemType.invalid, f'invalid id {chunk.id!r}')
				return

			chunk_path = chunk_utils.get_chunk_path(chunk.hash)
			if not chunk_path.is_file():
				result.add_bad(chunk, BadChunkItemType.missing_file, f'chunk file {chunk_path} does not exist')
				return

			try:
				compressor = Compressor.create(chunk.compress)
			except ValueError:
				result.add_bad(chunk, BadChunkItemType.invalid, f'unknown compress method {chunk.compress!r}')
				return

			try:
				with compressor.open_decompressed_bypassed(chunk_path) as (reader, f_decompressed):
					sah = hash_utils.calc_reader_size_and_hash(f_decompressed, hash_method=chunk_utils.get_hash_method())
			except Exception as e:
				result.add_bad(chunk, BadChunkItemType.corrupted, f'cannot read and decompress chunk file: ({type(e)} {e}')
				return

			file_size = reader.get_read_len()
			if file_size != chunk.stored_size:
				result.add_bad(chunk, BadChunkItemType.mismatched, f'stored size mismatch, expect {chunk.stored_size}, found {file_size}')
				return
			if sah.hash != chunk.hash:
				result.add_bad(chunk, BadChunkItemType.mismatched, f'hash mismatch, expect {chunk.hash}, found {sah.hash}')
				return
			if sah.size != chunk.raw_size:
				result.add_bad(chunk, BadChunkItemType.mismatched, f'raw size mismatch, expect {chunk.raw_size}, found {sah.size}')
				return

			# it's a good chunk
			id_to_good_chunks[chunk.id] = chunk

		def check_orphan_chunks():
			orphan_chunk_ids = set(session.filtered_orphan_chunk_ids(list(id_to_good_chunks.keys())))
			for chunk_id, chunk in id_to_good_chunks.items():
				if chunk_id in orphan_chunk_ids:
					result.add_bad(chunk, BadChunkItemType.orphan, f'orphan chunk with 0 associated file, id {chunk_id}, hash {chunk.hash}')
				else:
					result.ok += 1

		with FailFastBlockingThreadPool('validator') as pool:
			for c in chunks:
				if self.is_interrupted.is_set():
					break
				result.validated += 1
				pool.submit(validate_one_chunk, c)

		check_orphan_chunks()

	@override
	def run(self, *, session: Optional[DbSession] = None) -> ValidateChunksResult:
		self.logger.info('Chunk validation start')
		result = ValidateChunksResult()

		with contextlib.ExitStack() as es:
			if session is None:
				session = es.enter_context(DbAccess.open_session())

			result.total = session.get_chunk_count()
			cnt = 0
			for chunks in session.iterate_chunk_batch(batch_size=3000):
				if self.is_interrupted.is_set():
					break
				cnt += len(chunks)
				self.logger.info('Validating {} / {} chunks'.format(cnt, result.total))
				self.__validate(session, result, list(map(ChunkInfo.of, chunks)))

		self.logger.info('Chunk validation done: total {}, validated {}, ok {}, bad {}'.format(
			result.total, result.validated, result.ok, len(result.bad_chunks),
		))
		return result
