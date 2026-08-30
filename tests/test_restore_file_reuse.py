import dataclasses
import hashlib
import os
import stat
from pathlib import Path
from typing import Callable, Generator

import pytest

from prime_backup.action.create_backup_action import CreateBackupAction
from prime_backup.action.export_backup_action_directory import ExportBackupToDirectoryAction
from prime_backup.action.helpers import restore_file_reuser
from prime_backup.action.helpers.restore_file_reuser import RestoreFileReuser
from prime_backup.config.config import Config, set_config_instance
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.types.hash_method import HashMethod
from prime_backup.types.operator import Operator


_CONTENT = b'reusable content'
_RELATIVE_PATH = Path('world/file.dat')


@dataclasses.dataclass(frozen=True)
class _ReuserCase:
	reuser: RestoreFileReuser
	file: schema.File
	trash_path: Path
	destination_path: Path


def _sha256_file(path: Path) -> str:
	return hashlib.sha256(path.read_bytes()).hexdigest()


def _make_reuser_case(
		tmp_path: Path,
		monkeypatch: pytest.MonkeyPatch,
		*,
		backup_content: bytes = _CONTENT,
		apply_backup_attrs: Callable[[schema.File, Path], None] = lambda _file, _path: None,
) -> _ReuserCase:
	output_path = tmp_path / 'output'
	trash_bin_path = tmp_path / 'trash'
	trash_path = trash_bin_path / _RELATIVE_PATH
	destination_path = output_path / _RELATIVE_PATH
	trash_path.parent.mkdir(parents=True)
	destination_path.parent.mkdir(parents=True)
	trash_path.write_bytes(_CONTENT)

	st = trash_path.stat()
	backup_file = schema.File(
		mode=st.st_mode,
		uid=st.st_uid,
		gid=st.st_gid,
		mtime=st.st_mtime_ns // 10 ** 9,
		mtime_ns_part=st.st_mtime_ns % 10 ** 9,
		blob_hash=hashlib.sha256(backup_content).hexdigest(),
		blob_raw_size=len(backup_content),
	)
	monkeypatch.setattr(restore_file_reuser.hash_utils, 'calc_file_hash', _sha256_file)
	return _ReuserCase(
		reuser=RestoreFileReuser(
			output_path,
			trash_bin_path,
			apply_backup_attrs_func=apply_backup_attrs,
		),
		file=backup_file,
		trash_path=trash_path,
		destination_path=destination_path,
	)


def test_reuser_reuses_matching_file_and_rolls_it_back(
		tmp_path: Path,
		monkeypatch: pytest.MonkeyPatch,
) -> None:
	case = _make_reuser_case(tmp_path, monkeypatch)

	assert case.reuser.try_reuse(case.file, _RELATIVE_PATH)
	assert case.destination_path.read_bytes() == _CONTENT
	assert not case.trash_path.exists()

	case.reuser.rollback()
	assert case.trash_path.read_bytes() == _CONTENT
	assert not case.destination_path.exists()


def test_reuser_commit_makes_the_move_permanent(
		tmp_path: Path,
		monkeypatch: pytest.MonkeyPatch,
) -> None:
	case = _make_reuser_case(tmp_path, monkeypatch)

	assert case.reuser.try_reuse(case.file, _RELATIVE_PATH)
	case.reuser.commit()
	case.reuser.rollback()

	assert case.destination_path.read_bytes() == _CONTENT
	assert not case.trash_path.exists()


def test_reuser_falls_back_for_different_content(
		tmp_path: Path,
		monkeypatch: pytest.MonkeyPatch,
) -> None:
	case = _make_reuser_case(tmp_path, monkeypatch, backup_content=b'changed content!')

	assert not case.reuser.try_reuse(case.file, _RELATIVE_PATH)
	assert case.trash_path.read_bytes() == _CONTENT
	assert not case.destination_path.exists()


def test_reuser_falls_back_when_candidate_cannot_be_inspected(
		tmp_path: Path,
		monkeypatch: pytest.MonkeyPatch,
) -> None:
	case = _make_reuser_case(tmp_path, monkeypatch)

	def fail_hash(_path: Path) -> str:
		raise PermissionError('injected inspection failure')

	monkeypatch.setattr(restore_file_reuser.hash_utils, 'calc_file_hash', fail_hash)

	assert not case.reuser.try_reuse(case.file, _RELATIVE_PATH)
	assert case.trash_path.read_bytes() == _CONTENT
	assert not case.destination_path.exists()


def test_reuser_falls_back_when_candidate_cannot_be_moved(
		tmp_path: Path,
		monkeypatch: pytest.MonkeyPatch,
) -> None:
	case = _make_reuser_case(tmp_path, monkeypatch)
	original_replace = os.replace

	def fail_candidate_move(source: Path, destination: Path) -> None:
		if Path(source) == case.trash_path and Path(destination) == case.destination_path:
			raise PermissionError('injected move failure')
		original_replace(source, destination)

	monkeypatch.setattr(restore_file_reuser.os, 'replace', fail_candidate_move)

	assert not case.reuser.try_reuse(case.file, _RELATIVE_PATH)
	assert case.trash_path.read_bytes() == _CONTENT
	assert not case.destination_path.exists()


@dataclasses.dataclass(frozen=True)
class _RestoreEnv:
	server_path: Path
	world_path: Path


@pytest.fixture(name='restore_env')
def _restore_env(tmp_path: Path) -> Generator[_RestoreEnv, None, None]:
	old_config = Config.get()
	if DbAccess.is_initialized():
		DbAccess.shutdown()

	server_path = tmp_path / 'server'
	world_path = server_path / 'world'
	world_path.mkdir(parents=True)

	config = Config.get_default()
	config.storage_root = str(tmp_path / 'pb_files')
	config.backup.source_root = str(server_path)
	config.backup.hash_method = HashMethod.sha256
	set_config_instance(config)
	DbAccess.init_memory_db()

	try:
		yield _RestoreEnv(server_path, world_path)
	finally:
		if DbAccess.is_initialized():
			DbAccess.shutdown()
		set_config_instance(old_config)


def _read_regular_files(root: Path) -> dict[str, bytes]:
	return {
		path.relative_to(root).as_posix(): path.read_bytes()
		for path in root.rglob('*')
		if stat.S_ISREG(path.stat().st_mode)
	}


def test_restore_with_reuse_enabled_restores_the_backup_tree(restore_env: _RestoreEnv) -> None:
	backup_tree = {
		'unchanged.dat': b'unchanged',
		'changed.dat': b'from backup',
		'missing.dat': b'recreated',
	}
	for name, content in backup_tree.items():
		(restore_env.world_path / name).write_bytes(content)
	backup_id = CreateBackupAction(Operator.literal('test'), 'restore reuse test').run().id

	(restore_env.world_path / 'changed.dat').write_bytes(b'current data')
	(restore_env.world_path / 'missing.dat').unlink()
	(restore_env.world_path / 'extra.dat').write_bytes(b'remove me')
	Config.get().restore.reuse_unchanged_files = True

	failures = ExportBackupToDirectoryAction(
		backup_id,
		restore_env.server_path,
		restore_mode=True,
	).run()

	assert len(failures) == 0
	assert _read_regular_files(restore_env.world_path) == backup_tree
