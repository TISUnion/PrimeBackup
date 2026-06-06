from pathlib import Path
from typing import Dict, Generator, List, Tuple

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


def _param_id(value: object) -> str:
	if hasattr(value, 'name'):
		return str(getattr(value, 'name'))
	return 'c{}'.format(value)


@pytest.fixture(autouse=True)
def _restore_config_and_db() -> Generator[None, None, None]:
	old_config = Config.get()
	try:
		yield
	finally:
		if DbAccess.is_initialized():
			DbAccess.shutdown()
		set_config_instance(old_config)


def _assert_pack_and_chunk_validate_ok() -> None:
	assert ValidatePacksAction().run().bad == 0
	assert ValidateChunksAction().run().bad == 0


def _make_chunked_data() -> bytes:
	return (
		b'a' * (4 * 1024) +
		b'b' * (4 * 1024) +
		b'c' * (4 * 1024) +
		b'd' * (4 * 1024) +
		b'tail'
	)


def _populate_world(world_path: Path) -> Tuple[Dict[str, bytes], List[str]]:
	files = {
		'chunked.dat': _make_chunked_data(),
		'small.txt': b'hello roundtrip matrix',
		'empty.txt': b'',
		'config/settings.json': b'{"enabled":true,"level":3}\n',
		'logs/2026-06-06.log': b'line 1\nline 2\n',
		'nested/deeper/payload.bin': bytes(range(32)),
		'region/r.0.0.mca': b'mca-like small payload',
	}
	dirs = [
		'config',
		'logs',
		'nested',
		'nested/deeper',
		'region',
		'empty_dir',
		'empty_dir/child',
	]
	for rel_dir in dirs:
		(world_path / rel_dir).mkdir(parents=True, exist_ok=True)
	for rel_path, content in files.items():
		path = world_path / rel_path
		path.parent.mkdir(parents=True, exist_ok=True)
		path.write_bytes(content)
	return files, dirs


def __assert_restored_tree(restored_world_path: Path, expected_files: Dict[str, bytes], expected_dirs: List[str]) -> None:
	restored_files: Dict[str, bytes] = {
		path.relative_to(restored_world_path).as_posix(): path.read_bytes()
		for path in restored_world_path.rglob('*')
		if path.is_file()
	}
	restored_dirs: List[str] = [
		path.relative_to(restored_world_path).as_posix()
		for path in restored_world_path.rglob('*')
		if path.is_dir()
	]
	assert restored_files == expected_files
	assert sorted(restored_dirs) == sorted(expected_dirs)


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


def _get_export_extension(export_format: StandaloneBackupFormat) -> str:
	if isinstance(export_format.value, TarFormat):
		return export_format.value.value.extension
	return export_format.value.extension


def _make_config(
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


@pytest.mark.parametrize('hash_method', tuple(HashMethod), ids=_param_id)
@pytest.mark.parametrize('compress_method', tuple(CompressMethod), ids=_param_id)
@pytest.mark.parametrize('chunk_method', tuple(ChunkMethod), ids=_param_id)
@pytest.mark.parametrize('export_format', tuple(StandaloneBackupFormat), ids=_param_id)
@pytest.mark.parametrize('concurrency', (1, 2), ids=_param_id)
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
	imported_pb_path = tmp_path / 'imported_pb'
	server_path = tmp_path / 'server'
	world_path = server_path / 'world'
	restored_path = tmp_path / 'restored'
	export_path = tmp_path / ('backup' + _get_export_extension(export_format))

	world_path.mkdir(parents=True)
	expected_files, expected_dirs = _populate_world(world_path)

	set_config_instance(_make_config(source_pb_path, server_path, hash_method, compress_method, chunk_method, concurrency))
	DbAccess.init(create=True, migrate=False)
	try:
		backup = CreateBackupAction(Operator.literal('test'), '').run()
		_assert_pack_and_chunk_validate_ok()
		with DbAccess.open_session() as session:
			assert len(session.list_blobs_by_storage_method(BlobStorageMethod.chunked)) > 0

		__export_backup(backup.id, export_path, export_format)
		assert export_path.is_file()
		assert export_path.stat().st_size > 0
	finally:
		DbAccess.shutdown()

	set_config_instance(_make_config(imported_pb_path, server_path, hash_method, compress_method, chunk_method, concurrency))
	DbAccess.init(create=True, migrate=False)
	try:
		imported_backup = ImportBackupAction(export_path, export_format, ensure_meta=True).run()
		_assert_pack_and_chunk_validate_ok()

		failures = ExportBackupToDirectoryAction(imported_backup.id, restored_path, restore_mode=True).run()
		assert len(failures) == 0
		__assert_restored_tree(restored_path / 'world', expected_files, expected_dirs)
	finally:
		DbAccess.shutdown()
