import os
import stat
from pathlib import Path
from typing import List, Optional

from xbackup import utils
from xbackup.compressors import Compressor, CompressMethod
from xbackup.config.config import Config
from xbackup.db import schema
from xbackup.db.access import DbAccess
from xbackup.db.session import DbSession
from xbackup.task.task import Task


class BackUpTask(Task):
	def __init__(self, author: str, comment: str):
		super().__init__()
		self.author = author
		self.comment = comment
		self.__blobs_rollbackers: List[callable] = []
		self.backup_id: Optional[int] = None

	def scan_files(self) -> List[Path]:
		config = Config.get()
		collected = []

		for target in config.backup.targets:
			target_path = config.source_path / target
			if not target_path.exists():
				self.logger.info('skipping not-exist backup target {}'.format(target_path))
				continue

			if target_path.is_dir():
				for dir_path, dir_names, file_names in os.walk(target_path):
					for name in file_names + dir_names:
						collected.append(Path(dir_path) / name)
			else:
				collected.append(target_path)

		return [p for p in collected if not config.backup.is_file_ignore(p)]

	def __get_or_create_blob(self, session: DbSession, path: Path, st: os.stat_result) -> schema.Blob:
		# TODO: optimize read time
		# small files (<1M): all in memory, read once
		# files with unique size: read once, compress+hash to temp file, then move
		# file with duplicated size: read twice (slow)  <-------- current default
		h = utils.calc_file_hash(path)
		existing = session.get_blob(h)
		if existing is not None:
			return existing

		def rollback():
			try:
				if blob_path.is_file():
					os.remove(blob_path)
			except OSError as e:
				self.logger.error('(rollback) remove blob file {!r} failed: {}'.format(blob_path, e))

		blob_path = utils.get_blob_path(h)
		self.__blobs_rollbackers.append(rollback)

		if st.st_size < Config.get().backup.compress_threshold:
			method = CompressMethod.plain
		else:
			method = Config.get().backup.compress_method
		compressor = Compressor.create(method)
		compressor.compress(path, blob_path)

		return session.create_blob(hash=h, compress=method.name, size=st.st_size)

	def __create_file(self, session: DbSession, backup: schema.Backup, path: Path) -> schema.File:
		related_path = path.relative_to(Config.get().source_path)
		st = path.stat()

		if stat.S_ISDIR(st.st_mode):
			file_hash = None
		else:
			blob = self.__get_or_create_blob(session, path, st)
			file_hash = blob.hash

		return session.create_file(
			backup_id=backup.id,
			path=related_path.as_posix(),
			file_hash=file_hash,

			mode=st.st_mode,
			uid=st.st_uid,
			gid=st.st_gid,
			mtime_ns=st.st_mtime_ns,
			ctime_ns=st.st_ctime_ns,
		)

	def run(self):
		self.__blobs_rollbackers.clear()
		try:
			with DbAccess.open_session() as session:
				backup = session.create_backup(author=self.author, comment=self.comment)
				for p in self.scan_files():
					self.__create_file(session, backup, p)
				self.logger.info('create backup done, backup id {}'.format(backup.id))
				self.backup_id = backup.id
		except Exception as e:
			self.logger.info('error occurs, applying rollback')
			for rollback_func in self.__blobs_rollbackers:
				rollback_func()
			raise e
