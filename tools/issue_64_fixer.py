#!/usr/bin/env python3
"""
https://github.com/TISUnion/PrimeBackup/issues/64
Tested and works with Prime Backup v1.9.4
"""
import argparse
import collections
import logging
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

FilesetFileBackups = Dict[int, Dict[str, dict]]  # fileset_id -> (path -> row dict)


class DatabaseFilesetFileFixer:
	def __init__(self, bad_base_fileset_ids: List[int], db_backup_dir: Path):
		self.bad_base_fileset_ids = bad_base_fileset_ids
		self.db_backup_dir = db_backup_dir

		from prime_backup import logger
		self.logger: logging.Logger = logger.get()

	def __read_backup_db(self, db_path: Path) -> FilesetFileBackups:
		try:
			engine = create_engine('sqlite:///' + str(db_path))
			with Session(engine) as session:
				db_fileset_files: FilesetFileBackups = collections.defaultdict(dict)
				for fileset_id in self.bad_base_fileset_ids:
					file_rows = list(session.execute(text(f'SELECT * FROM file WHERE fileset_id = {fileset_id}')))
					for file_row in file_rows:
						# noinspection PyProtectedMember
						row = file_row._mapping
						db_fileset_files[fileset_id][row['path']] = dict(row)
					self.logger.info('Read {} file rows for fileset {} from database {!r}'.format(len(file_rows), fileset_id, str(db_path)))
				return db_fileset_files
		except Exception as e:
			self.logger.exception('Failed to read from db database {!r}: {}'.format(repr(str(db_path)), e))
			sys.exit(1)

	def __read_files_in_backup_dbs(self) -> FilesetFileBackups:
		if not self.db_backup_dir.is_dir():
			print('Invalid db backup directory: {}'.format(self.db_backup_dir))
			sys.exit(1)

		merged_file_backups: FilesetFileBackups = collections.defaultdict(dict)
		for file_name in os.listdir(self.db_backup_dir):
			file_path = self.db_backup_dir / file_name
			if file_path.is_file() and file_path.suffix == '.db':
				single_file_backups = self.__read_backup_db(file_path)
				for fileset_id, files in single_file_backups.items():
					merged_file_backups[fileset_id].update(files)

		return merged_file_backups

	def run(self):
		good_fileset_files = self.__read_files_in_backup_dbs()

		new_file_count = 0

		from prime_backup.db.access import DbAccess
		with DbAccess.open_session() as session:
			for fileset_id in self.bad_base_fileset_ids:
				self.logger.info('Recovering files for base fileset {}'.format(fileset_id))
				existing_file_paths = {file.path: file for file in session.get_fileset_files(fileset_id)}
				good_file_for_this_fileset = good_fileset_files.get(fileset_id, {})
				new_file_this_fileset_count = 0
				for path, file_row in good_file_for_this_fileset.items():
					if path not in existing_file_paths:
						self.logger.debug('Recovering file at {!r} for fileset {}'.format(path, fileset_id))
						session.add(session.create_file(**file_row))
						new_file_count += 1
						new_file_this_fileset_count += 1
					else:
						from prime_backup.db import schema
						existing_file: schema.File = existing_file_paths[path]
						if file_row != existing_file.to_dict():
							self.logger.error('ERROR: existing file data mismatch? Did you really provide the correct backup database file?')
							self.logger.error('fileset: {}, path: {}'.format(fileset_id, path))
							self.logger.error('DB Existing: {}'.format(existing_file.to_dict()))
							self.logger.error('From Backup: {}'.format(file_row))
							sys.exit(1)

				self.logger.info('Recovering {} files for fileset {}'.format(new_file_this_fileset_count, fileset_id))

		self.logger.info('Recovered {} files in total'.format(new_file_count))
		if new_file_count > 0:
			from prime_backup.action.validate_filesets_action import ValidateFilesetsAction
			self.logger.info('Perform another filesets validation since new files were added')
			result = ValidateFilesetsAction().run()
			if len(result.bad_filesets) == 0:
				self.logger.info('NICE, fileset fix done')
			else:
				self.logger.info('There are still some bad filesets')
				with DbAccess.open_session() as session:
					fileset_ids_by_kind: Dict[bool, List[int]] = collections.defaultdict(list)  # is_base -> fileset_ids
					filesets = session.get_filesets(list(result.bad_filesets.keys()))
					for fileset in filesets.values():
						fileset_ids_by_kind[fileset.base_id == 0].append(fileset.id)
				self.logger.info('Bad base filesets: {}'.format(', '.join(map(str, fileset_ids_by_kind[True]))))
				self.logger.info('Bad delta filesets: {}'.format(', '.join(map(str, fileset_ids_by_kind[False]))))


class DatabaseBlobFixer:
	def __init__(self, bad_base_fileset_ids: List[int]):
		self.bad_base_fileset_ids = bad_base_fileset_ids

		from prime_backup import logger
		self.logger: logging.Logger = logger.get()

	def run(self):
		from prime_backup.db.access import DbAccess

		self.logger.info('Iterating all file objects in the database to check if there is any missing blob object')
		recover_count_total = 0
		with DbAccess.open_session() as session:
			for files in session.iterate_file_batch(batch_size=1000):
				blob_hashes: Set[str] = set()
				all_blob_rows: Dict[str, dict] = {}  # hash -> blob row
				for file in files:
					if file.blob_hash is not None:
						blob_hashes.add(file.blob_hash)
						all_blob_rows[file.blob_hash] = {
							'hash': file.blob_hash,
							'compress': file.blob_compress,
							'raw_size': file.blob_raw_size,
							'stored_size': file.blob_stored_size,
						}

				existing_blobs = session.get_blobs(list(blob_hashes))
				for blob_hash in blob_hashes:
					if existing_blobs.get(blob_hash) is None:
						blob_row = all_blob_rows[blob_hash]
						session.add(session.create_blob(**blob_row))
						recover_count_total += 1

		self.logger.info('Recovered {} database blob objects in total'.format(recover_count_total))


class BlobFileFixer:
	def __init__(self, blobs_backup_dir: Path):
		self.blobs_backup_dir = blobs_backup_dir

		from prime_backup import logger
		self.logger: logging.Logger = logger.get()

	def __find_blob_store_roots(self) -> List[Path]:
		if not os.path.isdir(self.blobs_backup_dir):
			self.logger.error('blobs_backup_dir {!r} is not a directory'.format(str(self.blobs_backup_dir)))
			sys.exit(1)

		from prime_backup.db.access import DbAccess
		hash_method = DbAccess.get_hash_method()

		self.logger.info('Searching at {!r} for blob storage roots, for hash_method {} with hex length {}'.format(str(self.blobs_backup_dir), hash_method.name, hash_method.value.hex_length))
		blob_store_roots = []
		for name1 in os.listdir(self.blobs_backup_dir):
			path1 = self.blobs_backup_dir / name1
			if path1.is_dir():
				for name2 in os.listdir(path1):
					path2 = path1 / name2
					if path2.is_dir() and re.fullmatch(r'[0-9a-f]{2}', name2):
						blob_store_roots.append(path1)
						self.logger.info('Found blob store root at {!r}'.format(str(path1)))
						break

		self.logger.info('Found {} blob store roots in total'.format(len(blob_store_roots)))
		return blob_store_roots

	@classmethod
	def __try_locate_blob_file(cls, blob_store_roots: List[Path], hash_hex: str) -> Optional[Path]:
		if len(hash_hex) <= 2:
			raise ValueError(f'hash {hash_hex!r} too short')

		for blob_store_root in blob_store_roots:
			blob_file_path = blob_store_root / hash_hex[:2] / hash_hex
			if blob_file_path.is_file():
				return blob_file_path
		return None

	def run(self):
		blob_store_roots = self.__find_blob_store_roots()

		from prime_backup.db.access import DbAccess
		from prime_backup.utils import blob_utils
		prev_missing_blob_count = 0
		post_missing_blob_count = 0
		fixed_blob_count = 0

		self.logger.info('Iterating all blobs in the database to check if there is any missing blobs')
		with DbAccess.open_session() as session:
			for blobs in session.iterate_blob_batch(batch_size=1000):
				for blob in blobs:
					prod_blob_path = blob_utils.get_blob_path(blob.hash)
					if not prod_blob_path.exists():
						prev_missing_blob_count += 1
						recover_blob_path = self.__try_locate_blob_file(blob_store_roots, hash_hex=blob.hash)
						if recover_blob_path is not None and recover_blob_path.is_file():
							shutil.copyfile(recover_blob_path, prod_blob_path)
							fixed_blob_count += 1
						else:
							post_missing_blob_count += 1

		self.logger.info('Found {} blobs with their file missing, and recovered {} blob files in total'.format(prev_missing_blob_count, fixed_blob_count))
		if fixed_blob_count > 0:
			from prime_backup.action.validate_blobs_action import ValidateBlobsAction
			self.logger.info('Perform a blob validation since new files were added')
			result = ValidateBlobsAction().run()
			if result.bad == 0:
				self.logger.info('NICE, blob fix done')
			else:
				from prime_backup.config.config import Config
				self.logger.info('There are still {} bad blobs, see {} for more information'.format(result.bad, Config.get().storage_path / 'logs' / 'validate.log'))
				if len(result.missing) > 0:
					self.logger.info('There are still {} missing blobs'.format(len(result.missing)))
				if len(result.missing) == result.bad:
					self.logger.info('Thankfully all bad blobs are blob-with-missing-file')


class Issue64Fixer:
	def __init__(self, args: argparse.Namespace):
		from prime_backup import logger
		self.args = args
		self.logger: logging.Logger = logger.get()
		self.__init_pb_environment()

	def run(self):
		bad_base_fileset_ids = self.__get_bad_base_fileset_ids()
		if self.args.db_backup_dir:
			DatabaseFilesetFileFixer(bad_base_fileset_ids, Path(self.args.db_backup_dir)).run()
			DatabaseBlobFixer(bad_base_fileset_ids).run()
		else:
			self.logger.info('--db-backup-dir not provided, skipping database file object + blob object fixing')
		if self.args.blobs_backup_dir:
			BlobFileFixer(Path(self.args.blobs_backup_dir)).run()
		else:
			self.logger.info('--blobs-backup-dir not provided, skipping blob file fixing')

		self.logger.info('All fixing done. You can check the database state with MCDR command "!!pb database validate all"')

	def __init_pb_environment(self):
		from prime_backup.config.config import Config
		from prime_backup.db.access import DbAccess

		config = Config.get()
		config.storage_root = str(Path(self.args.pb_dir).as_posix())
		self.logger.info('Storage root set to {!r}'.format(config.storage_root))

		try:
			DbAccess.init(create=False, migrate=False)
		except Exception as e:
			self.logger.exception('Load database failed: {}'.format(e))
			sys.exit(1)

		config.backup.hash_method = DbAccess.get_hash_method()  # use the hash method from the db

	def __get_bad_base_fileset_ids(self) -> List[int]:
		from prime_backup.action.validate_filesets_action import ValidateFilesetsAction
		self.logger.info('Locating bad fileset ids')

		result = ValidateFilesetsAction().run()
		bad_fileset_ids = list(sorted(set(result.bad_filesets.keys())))

		self.logger.info('Found {} bad filesets'.format(len(bad_fileset_ids)))

		from prime_backup.db.access import DbAccess
		bad_base_fileset_ids: List[int] = []
		with DbAccess.open_session() as session:
			filesets = session.get_filesets(bad_fileset_ids)
			for fileset in filesets.values():
				if fileset.base_id == 0:
					bad_base_fileset_ids.append(fileset.id)
		self.logger.info('Found {} bad base filesets'.format(len(bad_base_fileset_ids)))
		self.logger.info(', '.join(map(str, bad_base_fileset_ids)))

		return bad_base_fileset_ids


def main():
	parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument('--pb-plugin', required=True, help='Path to the Prime Backup plugin file')
	parser.add_argument('--pb-dir', required=True, help='Path to the pb_files directory')
	parser.add_argument('--db-backup-dir', help='Path to the directory that contains database backup files. All .db files inside will be used. You can ignore this argument if you dont need to recover database file objects')
	parser.add_argument('--blobs-backup-dir', help='Path to the directory that contains blobs directory backups. Directories inside should be something like blobs1, blobs2, blobs3, where each directory is a valid blobs directory from pb_files/blobs. You can ignore this argument if you dont need to recover blobs')
	args = parser.parse_args()

	if not os.path.isfile(args.pb_plugin):
		print('Invalid plugin file: {}'.format(args.pb_plugin))
		sys.exit(1)
	sys.path.append(args.pb_plugin)
	try:
		import prime_backup
	except ImportError:
		print('Failed to import prime_backup, please ensure you provided a correct plugin path. pb_plugin: {!r}'.format(args.pb_plugin))
		sys.exit(1)

	fixer = Issue64Fixer(args)
	fixer.run()


if __name__ == '__main__':
	main()
