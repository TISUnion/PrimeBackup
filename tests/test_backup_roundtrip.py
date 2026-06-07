import dataclasses
from pathlib import Path
from typing import Dict, Generator, List

import pytest

from prime_backup.action.create_backup_action import CreateBackupAction
from prime_backup.action.export_backup_action_directory import ExportBackupToDirectoryAction
from prime_backup.action.export_backup_action_tar import ExportBackupToTarAction
from prime_backup.action.export_backup_action_zip import ExportBackupToZipAction
from prime_backup.action.import_backup_action import ImportBackupAction
from prime_backup.action.validate_chunks_action import ValidateChunksAction
from prime_backup.action.validate_packs_action import ValidatePacksAction
from prime_backup.compressors import CompressMethod
from prime_backup.config.backup_config import ChunkingRule
from prime_backup.config.config import Config, set_config_instance
from prime_backup.db.access import DbAccess
from prime_backup.db.values import BlobStorageMethod
from prime_backup.types.chunk_method import ChunkMethod
from prime_backup.types.hash_method import HashMethod
from prime_backup.types.operator import Operator
from prime_backup.types.standalone_backup_format import StandaloneBackupFormat
from prime_backup.types.tar_format import TarFormat


@dataclasses.dataclass(frozen=True)
class ExpectedTree:
	files: Dict[str, bytes]
	dirs: List[str]


@dataclasses.dataclass(frozen=True)
class BackupSnapshot:
	name: str
	backup_id: int
	expected_tree: ExpectedTree


@dataclasses.dataclass(frozen=True)
class BackupArchive:
	snapshot: BackupSnapshot
	export_path: Path


def __param_id(value: object) -> str:
	if hasattr(value, 'name'):
		return str(getattr(value, 'name'))
	return 'c{}'.format(value)


@pytest.fixture(autouse=True)
def __restore_config_and_db() -> Generator[None, None, None]:
	old_config = Config.get()
	try:
		yield
	finally:
		if DbAccess.is_initialized():
			DbAccess.shutdown()
		set_config_instance(old_config)


def __assert_pack_and_chunk_validate_ok() -> None:
	assert ValidatePacksAction().run().bad == 0
	assert ValidateChunksAction().run().bad == 0


def __make_chunked_data() -> bytes:
	return (
		b'a' * (64 * 1024) +
		b'b' * (64 * 1024) +
		b'c' * (64 * 1024) +
		b'd' * (64 * 1024) +
		b'tail'
	)


def __make_updated_chunked_data() -> bytes:
	return (
		b'a' * (64 * 1024) +
		b'b' * (64 * 1024) +
		b'x' * (64 * 1024) +
		b'd' * (64 * 1024) +
		b'tail-v2'
	)


def __write_world_files(world_path: Path, files: Dict[str, bytes]) -> None:
	for rel_path, content in files.items():
		path = world_path / rel_path
		path.parent.mkdir(parents=True, exist_ok=True)
		path.write_bytes(content)


def __read_tree(root_path: Path) -> ExpectedTree:
	return ExpectedTree(
		files={
			path.relative_to(root_path).as_posix(): path.read_bytes()
			for path in root_path.rglob('*')
			if path.is_file()
		},
		dirs=[
			path.relative_to(root_path).as_posix()
			for path in root_path.rglob('*')
			if path.is_dir()
		],
	)


def __populate_world_for_first_backup(world_path: Path) -> ExpectedTree:
	files = {
		'chunked.dat': __make_chunked_data(),
		'small.txt': b'hello roundtrip matrix',
		'empty.txt': b'',
		'config/settings.json': b'{"enabled":true,"level":3}\n',
		'logs/2026-06-06.log': b'line 1\nline 2\n',
		'nested/deeper/payload.bin': bytes(range(32)),
		'region/r.0.0.mca': b'mca-like small payload',
		'unchanged/reused.dat': b'reused chunked payload\n' * 256,
		'old_area/remove_me.txt': b'this file should disappear in backup 2',
	}
	dirs = [
		'config',
		'logs',
		'nested',
		'nested/deeper',
		'region',
		'unchanged',
		'old_area',
		'empty_dir',
		'empty_dir/child',
	]
	for rel_dir in dirs:
		(world_path / rel_dir).mkdir(parents=True, exist_ok=True)
	__write_world_files(world_path, files)
	return __read_tree(world_path)


def __mutate_world_for_second_backup(world_path: Path) -> ExpectedTree:
	(world_path / 'old_area' / 'remove_me.txt').unlink()
	(world_path / 'old_area').rmdir()
	(world_path / 'empty_dir' / 'child').rmdir()
	(world_path / 'empty_dir').rmdir()
	(world_path / 'logs' / '2026-06-06.log').unlink()

	__write_world_files(world_path, {
		'chunked.dat': __make_updated_chunked_data(),
		'config/settings.json': b'{"enabled":true,"level":4,"updated":true}\n',
		'logs/2026-06-07.log': b'line 3\nline 4\n',
		'nested/deeper/payload.bin': bytes(reversed(range(32))) + b'\nupdated',
		'new_branch/added.txt': b'new file in backup 2',
		'new_branch/deeper/notes.txt': b'nested file added in backup 2',
		'new_branch/chunked_added.dat': b'new chunked file\n' * 512,
		'region/r.0.1.mca': b'another mca-like payload',
	})
	(world_path / 'new_empty' / 'child').mkdir(parents=True)
	return __read_tree(world_path)


def __assert_restored_tree(restored_world_path: Path, expected_tree: ExpectedTree) -> None:
	assert restored_world_path.is_dir()
	restored_tree = __read_tree(restored_world_path)
	assert restored_tree.files == expected_tree.files
	assert sorted(restored_tree.dirs) == sorted(expected_tree.dirs)


def __assert_restored_backup(restored_path: Path, expected_tree: ExpectedTree) -> None:
	assert sorted(path.name for path in restored_path.iterdir()) == ['world']
	__assert_restored_tree(restored_path / 'world', expected_tree)


def __skip_if_dependencies_unavailable(
		hash_method: HashMethod,
		compress_method: CompressMethod,
		chunk_method: ChunkMethod,
		export_format: StandaloneBackupFormat,
) -> None:
	try:
		hash_method.value.ensure_lib()
	except ImportError as e:
		pytest.skip('hash dependency unavailable for {}: {}'.format(hash_method.name, e))

	try:
		compress_method.value.ensure_lib()
	except ImportError as e:
		pytest.skip('compress dependency unavailable for {}: {}'.format(compress_method.name, e))

	if export_format == StandaloneBackupFormat.tar_zst:
		try:
			CompressMethod.zstd.value.ensure_lib()
		except ImportError as e:
			pytest.skip('export dependency unavailable for {}: {}'.format(export_format.name, e))

	try:
		chunk_method.ensure_lib()
	except ImportError as e:
		pytest.skip('chunking dependency unavailable for {}: {}'.format(chunk_method.name, e))


def __get_export_extension(export_format: StandaloneBackupFormat) -> str:
	if isinstance(export_format.value, TarFormat):
		return export_format.value.value.extension
	return export_format.value.extension


def __make_config(
		storage_root: Path,
		server_path: Path,
		hash_method: HashMethod,
		compress_method: CompressMethod,
		chunk_method: ChunkMethod,
		concurrency: int,
) -> Config:
	config = Config.get_default()
	config.concurrency = concurrency
	config.storage_root = str(storage_root)
	config.backup.source_root = str(server_path)
	config.backup.targets = ['world']
	config.backup.hash_method = hash_method
	config.backup.compress_method = compress_method
	config.backup.compress_threshold = 0
	config.backup.chunking_enabled = True
	config.backup.chunking_rules = [
		ChunkingRule(algorithm=chunk_method, file_size_threshold=1, patterns=['**/*.dat']),
	]
	return config


def __export_backup(backup_id: int, export_path: Path, export_format: StandaloneBackupFormat) -> None:
	if isinstance(export_format.value, TarFormat):
		ExportBackupToTarAction(backup_id, export_path, export_format.value, create_meta=True).run()
	else:
		ExportBackupToZipAction(backup_id, export_path, create_meta=True).run()


def __assert_chunked_blob_exists() -> None:
	with DbAccess.open_session() as session:
		assert len(session.list_blobs_by_storage_method(BlobStorageMethod.chunked)) > 0


def __create_backup_snapshot(name: str, expected_tree: ExpectedTree) -> BackupSnapshot:
	backup = CreateBackupAction(Operator.literal('test'), name).run()
	__assert_pack_and_chunk_validate_ok()
	__assert_chunked_blob_exists()
	return BackupSnapshot(name, backup.id, expected_tree)


def __export_backup_snapshot(
		snapshot: BackupSnapshot,
		export_dir: Path,
		export_format: StandaloneBackupFormat,
) -> BackupArchive:
	export_path = export_dir / ('backup_{}{}'.format(snapshot.name, __get_export_extension(export_format)))
	__export_backup(snapshot.backup_id, export_path, export_format)
	assert export_path.is_file()
	assert export_path.stat().st_size > 0
	return BackupArchive(snapshot, export_path)


def __import_and_restore_backup_archive(
		archive: BackupArchive,
		imported_pb_path: Path,
		restored_path: Path,
		server_path: Path,
		hash_method: HashMethod,
		compress_method: CompressMethod,
		chunk_method: ChunkMethod,
		export_format: StandaloneBackupFormat,
		concurrency: int,
) -> None:
	set_config_instance(__make_config(imported_pb_path, server_path, hash_method, compress_method, chunk_method, concurrency))
	DbAccess.init(create=True, migrate=False)
	try:
		imported_backup = ImportBackupAction(archive.export_path, export_format, ensure_meta=True).run()
		__assert_pack_and_chunk_validate_ok()

		failures = ExportBackupToDirectoryAction(imported_backup.id, restored_path, restore_mode=True).run()
		assert len(failures) == 0
		__assert_restored_backup(restored_path, archive.snapshot.expected_tree)
	finally:
		DbAccess.shutdown()


@pytest.mark.parametrize('hash_method', tuple(HashMethod), ids=__param_id)
@pytest.mark.parametrize('compress_method', tuple(CompressMethod), ids=__param_id)
@pytest.mark.parametrize('chunk_method', tuple(ChunkMethod), ids=__param_id)
@pytest.mark.parametrize('export_format', tuple(StandaloneBackupFormat), ids=__param_id)
@pytest.mark.parametrize('concurrency', (1, 2), ids=__param_id)
def test_backup_roundtrip(
		tmp_path: Path,
		hash_method: HashMethod,
		compress_method: CompressMethod,
		chunk_method: ChunkMethod,
		export_format: StandaloneBackupFormat,
		concurrency: int,
) -> None:
	__skip_if_dependencies_unavailable(hash_method, compress_method, chunk_method, export_format)

	source_pb_path = tmp_path / 'source_pb'
	server_path = tmp_path / 'server'
	world_path = server_path / 'world'
	export_dir = tmp_path / 'exports'

	world_path.mkdir(parents=True)
	export_dir.mkdir()
	first_expected_tree = __populate_world_for_first_backup(world_path)

	set_config_instance(__make_config(source_pb_path, server_path, hash_method, compress_method, chunk_method, concurrency))
	DbAccess.init(create=True, migrate=False)
	try:
		first_snapshot = __create_backup_snapshot('first', first_expected_tree)
		second_expected_tree = __mutate_world_for_second_backup(world_path)
		second_snapshot = __create_backup_snapshot('second', second_expected_tree)
		archives = [
			__export_backup_snapshot(first_snapshot, export_dir, export_format),
			__export_backup_snapshot(second_snapshot, export_dir, export_format),
		]
	finally:
		DbAccess.shutdown()

	for archive in archives:
		__import_and_restore_backup_archive(
			archive,
			tmp_path / ('imported_pb_' + archive.snapshot.name),
			tmp_path / ('restored_' + archive.snapshot.name),
			server_path,
			hash_method,
			compress_method,
			chunk_method,
			export_format,
			concurrency,
		)
