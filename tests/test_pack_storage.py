import dataclasses
from io import BytesIO
from pathlib import Path
from typing import Dict, Generator, Optional, Tuple

import pytest

from prime_backup.action.compact_packs_action import CompactAllPacksAction
from prime_backup.action.create_backup_action import CreateBackupAction
from prime_backup.action.delete_backup_action import DeleteBackupAction
from prime_backup.action.delete_backup_file_action import DeleteBackupFileAction
from prime_backup.action.export_backup_action_tar import ExportBackupToTarAction
from prime_backup.action.get_pack_action import GetPackByFileNamePrefixAction, GetPackByIdAction
from prime_backup.action.helpers.chunk_io import ChunkIO
from prime_backup.action.helpers.pack_reader import PackEntryReader
from prime_backup.action.helpers.pack_writer import PackWriter
from prime_backup.action.import_backup_action import ImportBackupAction
from prime_backup.action.migrate_compress_method_action import MigrateCompressMethodAction
from prime_backup.action.scan_unknown_pack_files import ScanUnknownPackFilesAction
from prime_backup.action.validate_chunk_objects_action import ValidateChunkObjectsAction
from prime_backup.action.validate_chunks_action import ValidateChunksAction
from prime_backup.action.validate_packs_action import ValidatePacksAction
from prime_backup.compressors import CompressMethod
from prime_backup.config.backup_config import ChunkingRule
from prime_backup.config.config import Config, set_config_instance
from prime_backup.constants import pack_constants
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.exceptions import PackFileNameNotUnique
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.chunk_info import ChunkInfo
from prime_backup.types.chunk_method import ChunkMethod
from prime_backup.types.hash_method import HashMethod
from prime_backup.types.operator import Operator
from prime_backup.types.pack_info import PackChangeSummary, PackEntryLocation, PackInfo
from prime_backup.types.standalone_backup_format import StandaloneBackupFormat
from prime_backup.types.tar_format import TarFormat
from prime_backup.utils import hash_utils, pack_utils


@dataclasses.dataclass(frozen=True)
class PackStorageEnv:
	root: Path
	pb_path: Path
	server_path: Path
	world_path: Path
	export_path: Path


@pytest.fixture(name='env')
def __pack_storage_env(tmp_path: Path) -> Generator[PackStorageEnv, None, None]:
	old_config = Config.get()
	if DbAccess.is_initialized():
		DbAccess.shutdown()

	root = tmp_path / 'pack_storage'
	pb_path = root / 'pb_files'
	server_path = root / 'server'
	world_path = server_path / 'world'
	export_path = root / 'out.tar'

	world_path.mkdir(parents=True)
	(world_path / 'a.dat').write_bytes((b'a' * 9000 + b'b' * 9000) * 10)
	(world_path / 'b.dat').write_bytes((b'c' * 7000 + b'd' * 7000) * 12)
	(world_path / 'small.txt').write_text('hello pack', encoding='utf8')

	config = Config.get_default()
	set_config_instance(config)
	config.storage_root = str(pb_path)
	config.backup.source_root = str(server_path)
	config.backup.targets = ['world']
	config.backup.hash_method = HashMethod.xxh128
	config.backup.compress_method = CompressMethod.plain
	config.backup.compress_threshold = 1 << 60
	config.backup.chunking_enabled = True
	config.backup.chunking_rules = [
		ChunkingRule(algorithm=ChunkMethod.fixed_4k, file_size_threshold=1, patterns=['**/*.dat']),
	]
	config.backup.pack_auto_compact_threshold = 0.75
	DbAccess.init(create=True, migrate=False)

	try:
		yield PackStorageEnv(root, pb_path, server_path, world_path, export_path)
	finally:
		if DbAccess.is_initialized():
			DbAccess.shutdown()
		set_config_instance(old_config)


def __assert_pack_and_chunk_validate_ok() -> None:
	__assert_pack_db_files_consistent()
	assert ValidatePacksAction().run().bad == 0
	assert ValidateChunksAction().run().bad == 0


def __assert_pack_db_files_consistent() -> None:
	with DbAccess.open_session() as session:
		for pack in session.list_packs():
			assert pack_utils.get_pack_path(pack.id).stat().st_size == pack.size
			live_chunks = session.get_live_chunks_by_pack_id(pack.id)
			assert sum(chunk.stored_size for chunk in live_chunks) == pack.live_size
			assert len(live_chunks) == pack.live_entry_count
		for chunk in session.list_chunks():
			assert chunk.pack_id > 0
			assert chunk.pack_offset >= 0
			assert chunk.stored_size >= 0


def __create_backup() -> BackupInfo:
	return CreateBackupAction(Operator.literal('test'), '').run()


def __get_pack_stats() -> Dict[int, Tuple[int, int, int, int]]:
	with DbAccess.open_session() as session:
		return {
			pack.id: (pack.size, pack.entry_count, pack.live_size, pack.live_entry_count)
			for pack in session.list_packs()
		}


def __write_test_chunk(session: DbSession, pack_writer: PackWriter, data: bytes) -> PackEntryLocation:
	location = pack_writer.write_entry(data)
	session.create_and_add_chunk(
		hash=hash_utils.calc_bytes_hash(data),
		compress=CompressMethod.plain.name,
		raw_size=len(data),
		stored_size=len(data),
		pack_id=location.pack_id,
		pack_offset=location.offset,
	)
	return location


def test_pack_create_compact_export_import_and_unknown_file_cleanup(env: PackStorageEnv) -> None:
	backup = __create_backup()
	__assert_pack_and_chunk_validate_ok()

	with DbAccess.open_session() as session:
		pack_count_before = session.get_pack_count()
		chunk_count_before = session.get_chunk_count()
		pack_stats_before = session.get_pack_overview_stats()
	assert pack_count_before > 0
	assert chunk_count_before > 0

	delete_delta = DeleteBackupFileAction(backup.id, 'world/a.dat', allow_directory=False).run()
	assert delete_delta.freed_disk_size > 0
	assert delete_delta.packs.freed_size > 0
	__assert_pack_and_chunk_validate_ok()
	with DbAccess.open_session() as session:
		pack_stats_after_delete = session.get_pack_overview_stats()
	assert pack_stats_after_delete.size_sum <= pack_stats_before.size_sum

	ExportBackupToTarAction(backup.id, env.export_path, TarFormat.plain, create_meta=False).run()
	assert env.export_path.is_file()
	assert env.export_path.stat().st_size > 0

	unknown_path = pack_utils.get_pack_store() / 'aa' / ('aa' + '1' * 30)
	unknown_path.parent.mkdir(parents=True, exist_ok=True)
	unknown_path.write_bytes(b'orphan pack')
	scan_result = ScanUnknownPackFilesAction(delete=True, result_sample_limit=10).run()
	assert scan_result.count >= 1
	assert not unknown_path.exists()

	DbAccess.shutdown()
	Config.get().storage_root = str(env.root / 'imported_pb')
	DbAccess.init(create=True, migrate=False)
	imported_backup = ImportBackupAction(env.export_path, StandaloneBackupFormat.tar, ensure_meta=False).run()
	assert imported_backup.id == 1
	__assert_pack_and_chunk_validate_ok()


def test_compact_all_packs_with_full_threshold_reclaims_only_dead_space(env: PackStorageEnv) -> None:
	backup = __create_backup()
	pack_stats_before_clean_compact = __get_pack_stats()
	pack_paths_before_clean_compact = {
		pack_id: pack_utils.get_pack_path(pack_id)
		for pack_id in pack_stats_before_clean_compact
	}

	clean_summary = CompactAllPacksAction(threshold=1.0).run()
	assert clean_summary.changed_pack_count == 0
	assert clean_summary.freed_size == 0
	assert __get_pack_stats() == pack_stats_before_clean_compact
	for path in pack_paths_before_clean_compact.values():
		assert path.is_file()

	Config.get().backup.pack_auto_compact_threshold = 0
	delete_delta = DeleteBackupFileAction(backup.id, 'world/a.dat', allow_directory=False).run()
	assert delete_delta.chunk_count > 0
	assert delete_delta.packs.reclaimed_pack_count == 0

	with DbAccess.open_session() as session:
		dead_pack_infos = [
			PackInfo.of(pack)
			for pack in session.list_packs()
			if pack.live_size < pack.size
		]
	assert len(dead_pack_infos) > 0

	manual_summary = CompactAllPacksAction(threshold=1.0).run()
	assert manual_summary.reclaimed_pack_count == len(dead_pack_infos)
	assert manual_summary.freed_size > 0
	for pack_info in dead_pack_infos:
		assert not pack_info.file_path.exists()
	__assert_pack_and_chunk_validate_ok()


def test_new_backup_does_not_append_to_existing_packs(env: PackStorageEnv) -> None:
	backup_1 = __create_backup()
	pack_stats_before = __get_pack_stats()
	assert len(pack_stats_before) > 0

	(env.world_path / 'c.dat').write_bytes((b'e' * 11000 + b'f' * 11000) * 8)
	backup_2 = __create_backup()
	assert backup_2.id > backup_1.id

	pack_stats_after = __get_pack_stats()
	assert len(pack_stats_after) > len(pack_stats_before)
	for pack_id, old_stats in pack_stats_before.items():
		assert pack_stats_after[pack_id] == old_stats
	__assert_pack_and_chunk_validate_ok()


def test_pack_file_name_is_derived_from_pack_id(env: PackStorageEnv) -> None:
	__create_backup()

	with DbAccess.open_session() as session:
		packs = session.list_packs()
		assert len(packs) > 0
		for pack in packs:
			pack_info = PackInfo.of(pack)
			assert pack_info.file_name == pack_utils.get_pack_file_name(pack.id)
			assert pack_info.file_path == pack_utils.get_pack_path(pack.id)
			assert pack_info.file_path.is_file()


def test_get_pack_by_file_name_prefix_matches_and_rejects_ambiguity(env: PackStorageEnv) -> None:
	__create_backup()

	with DbAccess.open_session() as session:
		prefix_to_pack_id: Dict[str, int] = {}
		ambiguous_prefix: Optional[str] = None
		while ambiguous_prefix is None:
			pack_writer = PackWriter(session)
			data = 'prefix-candidate-{}'.format(len(prefix_to_pack_id)).encode('utf8')
			location = __write_test_chunk(session, pack_writer, data)
			pack_writer.close()
			prefix = pack_utils.get_pack_file_name(location.pack_id)[:1]
			if prefix in prefix_to_pack_id:
				ambiguous_prefix = prefix
			else:
				prefix_to_pack_id[prefix] = location.pack_id
		session.commit()

	with DbAccess.open_session() as session:
		pack = session.list_packs()[0]
		first_pack_id = pack.id
		full_name = PackInfo.of(pack).file_name

	assert GetPackByFileNamePrefixAction(full_name).run().id == first_pack_id
	assert GetPackByIdAction(first_pack_id).run().id == first_pack_id
	assert GetPackByFileNamePrefixAction(full_name[:8]).run().id == first_pack_id
	assert ambiguous_prefix is not None
	with pytest.raises(PackFileNameNotUnique):
		GetPackByFileNamePrefixAction(ambiguous_prefix).run()


def test_pack_change_summary_counts_created_updated_and_reclaimed_packs() -> None:
	summary = PackChangeSummary.zero()
	summary.created_pack_count += 2
	summary.updated_pack_count += 3
	summary.compacted_pack_count += 4
	summary.removed_pack_count += 5
	assert summary.reclaimed_pack_count == 9
	assert summary.changed_pack_count == 14
	assert summary.created_pack_count == 2
	assert summary.updated_pack_count == 3
	assert summary.compacted_pack_count == 4
	assert summary.removed_pack_count == 5


def test_pack_entry_reader_seek_uses_entry_relative_offsets() -> None:
	reader = PackEntryReader(BytesIO(b'0123456789abcdef'), 3, 8)

	assert reader.seekable()
	assert reader.read(3) == b'345'
	assert reader.seek(1) == 1
	assert reader.read(1) == b'4'
	assert reader.seek(3, 1) == 5
	assert reader.read(1) == b'8'
	assert reader.seek(-2, 2) == 6
	assert reader.read() == b'9a'
	assert reader.seek(100) == 8
	assert reader.read() == b''

	with pytest.raises(ValueError):
		reader.seek(-1)
	with pytest.raises(ValueError):
		reader.seek(0, 12345)


def test_scan_unknown_pack_files_keeps_known_derived_pack_file_names(env: PackStorageEnv) -> None:
	__create_backup()

	with DbAccess.open_session() as session:
		pack = session.list_packs()[0]
		known_pack_path = pack_utils.get_pack_path(pack.id)
		known_pack_size = known_pack_path.stat().st_size

	result = ScanUnknownPackFilesAction(delete=False, result_sample_limit=10).run()
	assert result.count == 0
	assert result.size == 0
	assert result.samples == []
	assert known_pack_path.is_file()
	assert known_pack_path.stat().st_size == known_pack_size

	unknown_path = pack_utils.get_pack_store() / 'ff' / ('ff' + '0' * 62)
	unknown_path.parent.mkdir(parents=True, exist_ok=True)
	unknown_path.write_bytes(b'orphan pack')
	result = ScanUnknownPackFilesAction(delete=False, result_sample_limit=1).run()
	assert result.count == 1
	assert result.size == len(b'orphan pack')
	assert len(result.samples) == 1
	assert result.samples[0].path == unknown_path
	assert result.samples[0].pack_file_name == unknown_path.name

	result = ScanUnknownPackFilesAction(delete=True, result_sample_limit=1).run()
	assert result.count == 1
	assert not unknown_path.exists()


def test_pack_writer_rotates_after_current_pack_reaches_max_size(env: PackStorageEnv) -> None:
	entry_size = pack_constants.PACK_MAX_SIZE // 3 + 1
	assert entry_size < pack_constants.PACK_DEDICATED_ENTRY_MIN_SIZE
	data_1 = b'x' * entry_size
	data_2 = b'y' * entry_size
	data_3 = b'z' * entry_size

	with DbAccess.open_session() as session:
		pack_writer = PackWriter(session)
		loc_1 = __write_test_chunk(session, pack_writer, data_1)
		loc_2 = __write_test_chunk(session, pack_writer, data_2)
		loc_3 = __write_test_chunk(session, pack_writer, data_3)
		loc_4 = __write_test_chunk(session, pack_writer, b'w')
		pack_writer.close()
		session.commit()

	assert loc_2.pack_id == loc_1.pack_id
	assert loc_3.pack_id == loc_1.pack_id
	assert loc_4.pack_id != loc_1.pack_id
	assert loc_1.offset == 0
	assert loc_2.offset == entry_size
	assert loc_3.offset == entry_size * 2
	assert loc_4.offset == 0

	with DbAccess.open_session() as session:
		first_pack = session.get_pack_by_id(loc_1.pack_id)
		second_pack = session.get_pack_by_id(loc_4.pack_id)
		assert first_pack.size == entry_size * 3
		assert first_pack.entry_count == 3
		assert second_pack.size == 1
		assert second_pack.entry_count == 1
		assert pack_utils.get_pack_path(first_pack.id).stat().st_size == first_pack.size
		assert pack_utils.get_pack_path(second_pack.id).stat().st_size == second_pack.size


def test_large_entry_can_be_stored_as_dedicated_pack(env: PackStorageEnv) -> None:
	data = b'x' * pack_constants.PACK_DEDICATED_ENTRY_MIN_SIZE
	active_before_data = b'a'
	active_after_data = b'b'
	data_hash = hash_utils.calc_bytes_hash(data)
	with DbAccess.open_session() as session:
		pack_writer = PackWriter(session)
		active_before_location = __write_test_chunk(session, pack_writer, active_before_data)
		entry_location = __write_test_chunk(session, pack_writer, data)
		active_after_location = __write_test_chunk(session, pack_writer, active_after_data)
		pack_writer.close()
		session.commit()

	assert active_after_location.pack_id == active_before_location.pack_id
	assert entry_location.pack_id != active_before_location.pack_id
	with DbAccess.open_session() as session:
		pack = session.get_pack_by_id(entry_location.pack_id)
		active_pack = session.get_pack_by_id(active_before_location.pack_id)
		assert active_pack.entry_count == 2
		assert active_pack.size == 2
		assert active_pack.live_size == 2
		assert pack_utils.get_pack_path(active_pack.id).stat().st_size == 2
		assert pack.entry_count == 1
		assert pack.size == len(data)
		assert pack.live_size == len(data)
		assert pack_utils.get_pack_path(pack.id).stat().st_size == len(data)
		chunk = session.get_chunk_by_hash(data_hash)
		chunk_info = ChunkInfo.of(chunk)

	read_data = ChunkIO(chunk_info).read_raw()
	assert len(read_data) == len(data)
	assert hash_utils.calc_bytes_hash(read_data) == data_hash
	assert ValidatePacksAction().run().bad == 0


def test_delete_backup_delta_includes_base_shrink_pack_compaction(env: PackStorageEnv) -> None:
	for i in range(12):
		(env.world_path / 'keep_{}.txt'.format(i)).write_text('keep {}'.format(i), encoding='utf8')
	backup_1 = __create_backup()

	(env.world_path / 'a.dat').unlink()
	backup_2 = __create_backup()
	assert backup_2.fileset_id_base == backup_1.fileset_id_base

	delete_result = DeleteBackupAction(backup_1.id).run()
	assert delete_result.delta.blob_count > 0
	assert delete_result.delta.chunk_count > 0
	assert delete_result.delta.freed_disk_size > 0
	assert delete_result.delta.packs.freed_size > 0
	__assert_pack_and_chunk_validate_ok()


def test_delete_delta_counts_updated_packs_without_compact(env: PackStorageEnv) -> None:
	backup = __create_backup()
	Config.get().backup.pack_auto_compact_threshold = 0

	delete_delta = DeleteBackupFileAction(backup.id, 'world/a.dat', allow_directory=False).run()
	assert delete_delta.chunk_count > 0
	assert delete_delta.packs.updated_pack_count > 0
	assert delete_delta.packs.reclaimed_pack_count == 0
	assert delete_delta.packs.changed_pack_count == delete_delta.packs.updated_pack_count
	__assert_pack_and_chunk_validate_ok()


def test_pack_validation_is_independent_from_chunk_objects_validation(env: PackStorageEnv) -> None:
	__create_backup()
	with DbAccess.open_session() as session:
		pack = session.list_packs()[0]
		pack.live_size += 1
		session.commit()

	pack_result = ValidatePacksAction().run()
	assert pack_result.bad == 1

	result = ValidateChunkObjectsAction().run()
	assert result.chunk_result.bad == 0
	assert result.affected_blob_count == 0
	assert result.affected_file_count == 0


def test_migrate_compress_method_rewrites_pack_entries_by_pack(env: PackStorageEnv) -> None:
	__create_backup()
	with DbAccess.open_session() as session:
		old_pack_file_names = {PackInfo.of(pack).file_name for pack in session.list_packs()}
		old_pack_count = len(old_pack_file_names)
		old_chunk_count = session.get_chunk_count()

	Config.get().backup.compress_threshold = 0
	diff = MigrateCompressMethodAction(CompressMethod.gzip).run()
	assert diff.after != diff.before
	__assert_pack_and_chunk_validate_ok()

	with DbAccess.open_session() as session:
		packs = session.list_packs()
		assert session.get_chunk_count() == old_chunk_count
		assert len(packs) == old_pack_count
		assert all(PackInfo.of(pack).file_name not in old_pack_file_names for pack in packs)
		assert all(chunk.compress == CompressMethod.gzip.name for chunk in session.list_chunks())
