from typing import NamedTuple

from mcdreforged.minecraft.rtext.text import RTextBase

from xbackup.db import schema
from xbackup.task.types.operator import Operator
from xbackup.utils import conversion_utils


class BackupInfo(NamedTuple):
	id: int
	timestamp_ns: int
	date: str
	author: Operator
	comment: str
	size: int  # actual uncompressed size

	@classmethod
	def of(cls, backup: schema.Backup) -> 'BackupInfo':
		"""
		Notes: should be inside a session
		"""
		size_sum = 0
		for file in backup.files:
			if file.blob_size is not None:
				size_sum += file.blob_size
		return BackupInfo(
			id=backup.id,
			timestamp_ns=backup.timestamp,
			date=conversion_utils.timestamp_to_local_date(backup.timestamp, decimal=False),
			author=Operator.of(backup.author),
			comment=backup.comment,
			size=size_sum,
		)

	def pretty_text(self, with_buttons: bool) -> RTextBase:
		from xbackup.utils.mcdr_utils import tr
		# TODO
		base_info = tr('pretty_backup.base', self.id, self.date, self.comment)
		return base_info
