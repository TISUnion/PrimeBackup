import os
from pathlib import Path
from typing import List, NamedTuple

from mcdreforged.api.all import CommandSource

from xbackup import schema, utils
from xbackup.config.config import Config
from xbackup.dao import DAO
from xbackup.task.task import Task


class BackUpTask(Task):
	def __init__(self, source: CommandSource, comment: str):
		super().__init__(source)
		self.comment = comment

	def scan_files(self) -> List[Path]:
		config = Config.get()
		collected = []

		for target in config.backup.targets:
			target_path = config.source_path / target
			if not target_path.exists():
				self.logger.info('skipping not-exist backup target {!r}'.format(target_path))
				continue

			if target_path.is_dir():
				for dir_path, dir_names, file_names in os.walk(target_path):
					for name in file_names + dir_names:
						collected.append(Path(dir_path) / name)
			else:
				collected.append(target_path)

		return [p for p in collected if not config.backup.is_file_ignore(p)]

	def get_or_create_blob(self, path: Path, stat: os.stat_result) -> schema.Blob:
		# TODO: optimize read time
		# small files (<1M): all in memory, read once
		# files with unique size: read once, compress+hash to temp file, then move
		# file with duplicated size: read twice (slow)  <-------- current default
		h = utils.calc_file_hash(path)
		existing = DAO.get_blob(h)
		if existing is not None:
			# TODO: should we need hash check here?
			return existing


	def create_file(self, backup: schema.Backup, path: Path) -> schema.File:
		related_path = path.relative_to(Config.get().source_path)
		stat = path.stat()

		blob = self.get_or_create_blob(path, stat)

		return schema.File(
			hash=blob.hash,
			backup_id=backup.id,

			path=str(related_path),
			mode=stat.st_mode,
			uid=stat.st_uid,
			gid=stat.st_gid,
			mtime_ns=stat.st_mtime_ns,
			ctime_ns=stat.st_ctime_ns,
		)

	def run(self):
		backup = DAO.create_backup(self.comment)
		for p in self.scan_files():
			file = self.create_file(backup, p)

