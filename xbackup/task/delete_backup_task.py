import os
import shutil
import threading
from pathlib import Path
from typing import Optional, NamedTuple, List

from xbackup import utils
from xbackup.config.config import Config
from xbackup.db import schema
from xbackup.db.access import DbAccess
from xbackup.db.session import DbSession
from xbackup.task.task import Task


class BlobTrashBin:
	class Trash(NamedTuple):
		hash: str
		raw_size: int
		trash_path: Path
		origin_path: Path

	class Summary(NamedTuple):
		count: int
		raw_size_sum: int
		actual_size_sum: int

	def __init__(self, bin_path: Path):
		self.bin_path = bin_path
		self.trashes: List[BlobTrashBin.Trash] = []

	def add(self, blob: schema.Blob):
		blob_path = utils.get_blob_path(blob.hash)
		trash_path = self.bin_path / blob.hash
		trash_path.parent.mkdir(parents=True, exist_ok=True)

		shutil.move(blob_path, trash_path)
		self.trashes.append(self.Trash(blob.hash, blob.size, trash_path=trash_path, origin_path=blob_path))

	def make_summary(self) -> Summary:
		raw_size_sum, actual_size_sum = 0, 0
		for trash in self.trashes:
			raw_size_sum += trash.raw_size
			actual_size_sum += os.stat(trash.trash_path).st_size
		return self.Summary(len(self.trashes), raw_size_sum, actual_size_sum)

	def erase_all(self):
		for trash in self.trashes:
			os.remove(trash.trash_path)
		self.trashes.clear()

	def restore_all(self):
		for trash in self.trashes:
			shutil.move(trash.trash_path, trash.origin_path)
		self.trashes.clear()

	def delete_trash_bin(self):
		if self.bin_path.is_dir():
			shutil.rmtree(self.bin_path)


class DeleteBackupTask(Task):
	def __init__(self, backup_id: int):
		super().__init__()
		self.backup_id = backup_id
		self.trash_bin: Optional[BlobTrashBin] = None

	def run(self):
		self.trash_bin = BlobTrashBin(Config.get().storage_path / 'temp' / 'trash_bin_{}'.format(threading.current_thread().ident))
		try:
			with DbAccess.open_session() as session:
				backup = session.get_backup(self.backup_id)
				if backup is None:
					raise KeyError('backup with id {} not found'.format(self.backup_id))
				self.__delete_backup(session, backup)
				session.session.flush()
				session.delete_backup(backup)
		except Exception:
			self.logger.error('delete backup failed, restoring blobs')
			self.trash_bin.restore_all()
			raise
		else:
			summary = self.trash_bin.make_summary()
			self.logger.info('delete backup done, erasing blobs (count {}, size {} / {})'.format(summary.count, summary.actual_size_sum, summary.raw_size_sum))
			self.trash_bin.erase_all()
		finally:
			self.trash_bin.delete_trash_bin()

	def __delete_backup(self, session: DbSession, backup: schema.Backup):
		for file in backup.files:
			blob: Optional[schema.Blob] = file.blob
			session.delete_file(file)
			if blob is not None and len(blob.files) == 0:
				self.trash_bin.add(blob)
				session.delete_blob(blob)
