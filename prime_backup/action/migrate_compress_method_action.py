import shutil
import time
from concurrent.futures import Future
from pathlib import Path
from typing import List, Tuple, Set, Dict

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.action.compact_packs_action import CollectCompactablePacksStep, CompactPacksAction
from prime_backup.action.helpers.pack_reader import PackReader
from prime_backup.action.helpers.pack_writer import PackWriter
from prime_backup.compressors import CompressMethod, Compressor
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.db.values import BlobStorageMethod
from prime_backup.types.size_diff import SizeDiff
from prime_backup.utils import blob_utils, file_utils, pack_utils
from prime_backup.utils.bypass_io import BypassWriter
from prime_backup.utils.io_types import SupportsReadBytes
from prime_backup.utils.thread_pool import FailFastBlockingThreadPool

_OLD_BLOB_SUFFIX = '_old'


class MigrateCompressMethodAction(Action[SizeDiff]):
	def __init__(self, new_compress_method: CompressMethod):
		super().__init__()
		self.new_compress_method = new_compress_method

		# records changed files
		self.__migrate_started_blob_hashes: List[str] = []
		self.__migrated_blob_hashes: List[str] = []
		self.__migrated_chunk_hashes: List[str] = []

		# records affects stuffs for next migration phase
		self.__affected_chunk_groups_ids: Set[int] = set()
		self.__affected_fileset_ids: Set[int] = set()
		self.__old_pack_ids: Set[int] = set()
		self.__new_pack_paths: List[Path] = []

	def __update_files_for_blob_change(self, session: DbSession, changed_blobs_by_hash: Dict[str, schema.Blob]):
		for file in session.get_file_by_blob_hashes(list(changed_blobs_by_hash.keys())):
			if file.blob_hash is None:
				raise AssertionError('File {!r} has no blob_hash'.format(file))
			blob = changed_blobs_by_hash[file.blob_hash]
			file.blob_compress = blob.compress
			file.blob_stored_size = blob.stored_size
			self.__affected_fileset_ids.add(file.fileset_id)

	@classmethod
	def __get_blob_paths(cls, h: str) -> Tuple[Path, Path]:
		blob_path = blob_utils.get_blob_path(h)
		old_trash_path = blob_path.parent / (blob_path.name + _OLD_BLOB_SUFFIX)
		return blob_path, old_trash_path

	@classmethod
	def __compress_to_temp(cls, f_src: SupportsReadBytes, temp_path: Path, compressor: Compressor, *, estimate_read_size: int) -> int:
		with open(temp_path, 'wb') as f_out:
			writer = BypassWriter(f_out)
			with compressor.compress_stream(writer) as f_dst:
				file_utils.copy_file_obj_fast(f_src, f_dst, estimate_read_size=estimate_read_size)
			return writer.get_write_len()

	def __migrate_single_direct_blob(self, blob: schema.Blob) -> bool:
		new_compress_method = self.config.backup.get_compress_method_from_size(blob.raw_size, compress_method_override=self.new_compress_method)
		decompressor = Compressor.create(blob.compress)
		compressor = Compressor.create(new_compress_method)
		if decompressor.get_method() == compressor.get_method():
			return False

		blob_path, old_trash_path = self.__get_blob_paths(blob.hash)
		blob_path.replace(old_trash_path)
		self.__migrate_started_blob_hashes.append(blob.hash)
		with decompressor.open_decompressed(old_trash_path) as f_src:
			with compressor.open_compressed_bypassed(blob_path) as (writer, f_dst):
				file_utils.copy_file_obj_fast(f_src, f_dst, estimate_read_size=blob.stored_size)

		blob.compress = new_compress_method.name
		blob.stored_size = writer.get_write_len()
		return True

	def __migrate_direct_blobs_and_sync_files(self, session: DbSession, blobs: List[schema.Blob]):
		changed_blobs_by_hash: Dict[str, schema.Blob] = {}

		with FailFastBlockingThreadPool('migration') as pool:
			future_pairs: List[Tuple[schema.Blob, 'Future[bool]']] = []
			for blob in blobs:
				future = pool.submit(self.__migrate_single_direct_blob, blob)
				future_pairs.append((blob, future))
			for blob, future in future_pairs:
				try:
					changed = future.result()
				except Exception as e:
					self.logger.error('Migrate blob {} failed: {}'.format(blob, e))
					raise
				if changed:
					changed_blobs_by_hash[blob.hash] = blob
					self.__migrated_blob_hashes.append(blob.hash)

		self.__update_files_for_blob_change(session, changed_blobs_by_hash)

	def __compress_pack_entry_to_temp(self, pack_id: int, chunk: schema.Chunk, temp_path: Path, new_compress_method: CompressMethod) -> int:
		decompressor = Compressor.create(chunk.compress)
		compressor = Compressor.create(new_compress_method)
		with PackReader.open_entry(pack_id, chunk.pack_offset, chunk.stored_size) as entry_reader:
			with decompressor.decompress_stream(entry_reader) as f_src:
				return self.__compress_to_temp(f_src, temp_path, compressor, estimate_read_size=chunk.raw_size)

	def __get_new_chunk_compress_method(self, chunk: schema.Chunk) -> CompressMethod:
		new_compress_method = self.config.backup.get_compress_method_from_size(chunk.raw_size, compress_method_override=self.new_compress_method)
		return Compressor.create(new_compress_method).get_method()

	def __migrate_pack_entries_for_chunks(self, session: DbSession, pack: schema.Pack, temp_dir: Path, pack_writer: PackWriter):
		chunks = session.get_live_chunks_by_pack_id(pack.id)
		if len(chunks) == 0:
			return

		new_compress_methods = {
			chunk.id: self.__get_new_chunk_compress_method(chunk)
			for chunk in chunks
		}
		if all(chunk.compress == new_compress_methods[chunk.id].name for chunk in chunks):
			return

		changed_chunk_ids: Set[int] = set()
		temp_paths: List[Path] = []

		try:
			for chunk in chunks:
				old_stored_size = chunk.stored_size
				new_compress_method = new_compress_methods[chunk.id]
				if chunk.compress == new_compress_method.name:
					with PackReader.open_entry(pack.id, chunk.pack_offset, chunk.stored_size) as entry_reader:
						entry_location = pack_writer.write_entry_from_reader(entry_reader, chunk.stored_size)
				else:
					temp_path = temp_dir / '{}.tmp'.format(chunk.id)
					temp_paths.append(temp_path)
					try:
						new_stored_size = self.__compress_pack_entry_to_temp(pack.id, chunk, temp_path, new_compress_method)
					except Exception as e:
						self.logger.error('Migrate pack entry for chunk {} failed: {}'.format(chunk, e))
						raise
					with open(temp_path, 'rb') as f:
						entry_location = pack_writer.write_entry_from_reader(f, new_stored_size)
					chunk.compress = new_compress_method.name
					chunk.stored_size = new_stored_size
					changed_chunk_ids.add(chunk.id)
					self.__migrated_chunk_hashes.append(chunk.hash)
					temp_path.unlink(missing_ok=True)

				chunk.pack_id = entry_location.pack_id
				chunk.pack_offset = entry_location.offset
				pack.live_size -= old_stored_size
				pack.live_entry_count -= 1

			self.__old_pack_ids.add(pack.id)
		except Exception:
			raise
		finally:
			for temp_path in temp_paths:
				temp_path.unlink(missing_ok=True)

		self.__affected_chunk_groups_ids.update(session.get_chunk_group_ids_by_chunk_ids(list(changed_chunk_ids)))

	def __migrate_chunks_by_pack(self, session: DbSession, temp_dir: Path):
		pack_writer = PackWriter(session)
		try:
			packs = session.list_packs()
			for i, pack in enumerate(packs):
				self.logger.info('Processing pack entries {} / {}'.format(i + 1, len(packs)))
				self.__migrate_pack_entries_for_chunks(session, pack, temp_dir, pack_writer)
				session.flush()

			pack_writer.close()
			self.__new_pack_paths.extend(pack_writer.get_rollback_paths())
		except Exception:
			pack_writer.close()
			for pack_path in pack_writer.get_rollback_paths():
				pack_path.unlink(missing_ok=True)
			raise

	def __update_chunk_group_and_blobs_for_chunk_changes(self, session: DbSession):
		if len(self.__affected_chunk_groups_ids) == 0:
			return

		chunk_group_id_list = list(self.__affected_chunk_groups_ids)
		chunk_groups = session.get_chunk_groups_by_ids(chunk_group_id_list)
		for chunk_group_id, chunk_group in chunk_groups.items():
			if chunk_group is None:
				raise AssertionError('Chunk group with id {!r} does not exists'.format(chunk_group_id))
			chunk_group.chunk_stored_size_sum = session.calc_chunk_group_stored_size_sum(chunk_group.id)

		affected_blobs = session.get_blobs_by_chunk_group_ids(chunk_group_id_list)
		for blob in affected_blobs:
			if blob.storage_method != BlobStorageMethod.chunked.value:
				raise AssertionError('Blob {!r} is not a chunked blob'.format(blob))
			blob.stored_size = session.calc_chunked_blob_stored_size_sum(blob.id)
		self.__update_files_for_blob_change(session, {blob.hash: blob for blob in affected_blobs})

		self.logger.info('Syncing {} affected chunk groups and {} affected blobs for chunk changes'.format(len(chunk_groups), len(affected_blobs)))
		session.flush_and_expunge_all()

	def __update_fileset_and_backups(self, session: DbSession):
		if len(self.__affected_fileset_ids) == 0:
			return

		fileset_ids = set(self.__affected_fileset_ids)
		backup_ids = session.get_backup_ids_by_fileset_ids(list(fileset_ids))
		self.logger.info('Syncing {} affected filesets and {} associated backups'.format(len(fileset_ids), len(backup_ids)))

		filesets = session.get_filesets(list(fileset_ids))
		for fileset in filesets.values():
			fileset.file_stored_size_sum = session.calc_file_stored_size_sum(fileset.id)

		all_backup_fileset_ids: Set[int] = set()
		backups = list(session.get_backups(backup_ids).values())
		for backup in backups:
			all_backup_fileset_ids.add(backup.fileset_id_base)
			all_backup_fileset_ids.add(backup.fileset_id_delta)
		more_filesets = session.get_filesets(list(all_backup_fileset_ids.difference(fileset_ids)))
		filesets.update(more_filesets)  # batch query, faster

		for backup in backups:
			fs_base = filesets[backup.fileset_id_base]
			fs_delta = filesets[backup.fileset_id_delta]
			backup.file_stored_size_sum = fs_base.file_stored_size_sum + fs_delta.file_stored_size_sum
		session.flush_and_expunge_all()

	def __erase_old_blob_and_chunk_files(self):
		for h in self.__migrated_blob_hashes:
			_, old_trash_path = self.__get_blob_paths(h)
			old_trash_path.unlink()

	def __rollback(self):
		for h in self.__migrate_started_blob_hashes:
			blob_path, old_trash_path = self.__get_blob_paths(h)
			if old_trash_path.is_file():
				old_trash_path.replace(blob_path)
		for pack_path in self.__new_pack_paths:
			pack_path.unlink(missing_ok=True)

	@override
	def run(self) -> SizeDiff:
		# Notes: requires 2x disk usage of the blob store, stores all blob hashes in memory
		self.logger.info('Migrating compress method to {} (compress threshold = {})'.format(self.new_compress_method.name, self.config.backup.compress_threshold))

		db_committed = False
		before_size = 0
		after_size = 0
		try:
			# Blob operation steps:
			# 1. move xxx -> xxx_old
			# 2. copy xxx_old --[migrate]-> xxx
			# 3. delete xxx_old
			with DbAccess.open_session() as session:
				# 0. fetch information before the migration
				t = time.time()
				before_size = session.get_blob_store_fs_file_size_sum()
				pack_utils.prepare_pack_store()

				# 1. migrate direct blob objects
				cnt = 0
				total_blob_count = session.get_blob_count()
				for blobs in session.iterate_blob_batch(batch_size=1000):
					cnt += len(blobs)
					self.logger.info('Processing blobs {} / {}'.format(cnt, total_blob_count))
					self.__migrate_direct_blobs_and_sync_files(session, [
						blob for blob in blobs
						if blob.storage_method == BlobStorageMethod.direct.value
					])
					session.flush_and_expunge_all()
				if len(self.__migrated_blob_hashes) == 0:
					self.logger.info('No blob needs a compress method change, nothing to migrate')
				else:
					self.logger.info('Migrated {} direct blobs and related files'.format(len(self.__migrated_blob_hashes)))

				# 2. migrate chunks by pack, then sync chunk groups and chunked blobs
				temp_dir = self.config.temp_path / 'migrate_compress_chunks'
				temp_dir.mkdir(parents=True, exist_ok=True)
				self.__migrate_chunks_by_pack(session, temp_dir)
				if len(self.__migrated_chunk_hashes) == 0:
					self.logger.info('No chunk needs a compress method change, nothing to migrate')
				else:
					self.logger.info('Migrated {} chunks and related blob and files'.format(len(self.__migrated_chunk_hashes)))

				# 3. migrate affected fileset and backup data
				self.__update_chunk_group_and_blobs_for_chunk_changes(session)
				self.__update_fileset_and_backups(session)
				pack_ids_to_compact = CollectCompactablePacksStep(
					session,
					pack_ids=self.__old_pack_ids,
					threshold=self.config.backup.pack_auto_compact_threshold,
					raise_if_not_found=False,
				).run().pack_ids
				if len(pack_ids_to_compact) > 0:
					CompactPacksAction(pack_ids_to_compact, raise_if_not_found=False).run(session=session)
				else:
					session.commit()
			db_committed = True
			with DbAccess.open_session() as session:
				after_size = session.get_blob_store_fs_file_size_sum()

		except Exception:
			self.logger.warning('Error occurs during compress method migration, applying rollback')
			if not db_committed:
				self.__rollback()
			raise

		else:
			# 5. migration done, do some cleanup
			self.logger.info('Cleaning up old direct blob files')
			self.__erase_old_blob_and_chunk_files()

			self.config.backup.compress_method = self.new_compress_method
			self.logger.info('Compress method migration done, cost {}s'.format(round(time.time() - t, 2)))
			return SizeDiff(before_size, after_size)

		finally:
			shutil.rmtree(self.config.temp_path / 'migrate_compress_chunks', ignore_errors=True)
			self.__migrated_blob_hashes.clear()
			self.__migrated_chunk_hashes.clear()
