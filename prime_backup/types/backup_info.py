from typing import NamedTuple

from prime_backup.db import schema
from prime_backup.types.operator import Operator
from prime_backup.utils import conversion_utils


class BackupInfo(NamedTuple):
	id: int
	timestamp_ns: int
	date: str
	author: Operator
	comment: str
	size: int  # actual uncompressed size
	hidden: bool

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
			date=conversion_utils.timestamp_to_local_date(backup.timestamp),
			author=Operator.of(backup.author),
			comment=backup.comment,
			size=size_sum,
			hidden=backup.hidden,
		)
