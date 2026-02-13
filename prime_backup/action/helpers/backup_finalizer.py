import logging
from typing import List

from prime_backup.action.helpers.fileset_allocator import FilesetAllocateArgs, FilesetAllocator
from prime_backup.db import schema
from prime_backup.db.session import DbSession


class BackupFinalizer:
	def __init__(self, session: DbSession):
		from prime_backup import logger
		from prime_backup.config.config import Config
		self.logger: logging.Logger = logger.get()
		self.config: Config = Config.get()

		self.session = session

	def finalize_files_and_backup(self, backup: schema.Backup, files: List[schema.File]):
		self.session.flush()  # ensure all blobs has their blob.id allocated

		file_blobs = self.session.get_blobs_by_hashes([
			file.blob_hash for file in files
			if file.blob_hash is not None
		])
		for file in files:
			if file.blob_hash is not None:
				file_blob = file_blobs[file.blob_hash]
				if file_blob is None:
					raise AssertionError('blob of file does not exists: {}'.format(file))
				file.blob_id = file_blob.id

		allocate_args = FilesetAllocateArgs.from_config(self.config)
		allocate_result = FilesetAllocator(self.session, files).allocate(allocate_args)
		fs_base, fs_delta = allocate_result.fileset_base, allocate_result.fileset_delta

		backup.fileset_id_base = fs_base.id
		backup.fileset_id_delta = fs_delta.id
		backup.file_count = fs_base.file_count + fs_delta.file_count
		backup.file_raw_size_sum = fs_base.file_raw_size_sum + fs_delta.file_raw_size_sum
		backup.file_stored_size_sum = fs_base.file_stored_size_sum + fs_delta.file_stored_size_sum

		self.session.add(backup)
		self.session.flush()  # this generates backup.id
