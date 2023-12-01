import functools
from typing import NamedTuple

from prime_backup.db import schema
from prime_backup.types.operator import Operator
from prime_backup.utils import conversion_utils


class BackupInfo(NamedTuple):
	id: int
	timestamp_ns: int
	author: Operator
	comment: str
	hidden: bool
	raw_size: int  # uncompressed size
	stored_size: int  # actual size

	@functools.cached_property
	def date(self) -> str:
		return conversion_utils.timestamp_to_local_date(self.timestamp_ns)

	@classmethod
	def of(cls, backup: schema.Backup) -> 'BackupInfo':
		"""
		Notes: should be inside a session
		"""
		raw_size_sum, stored_size_sum = 0, 0
		for file in backup.files:
			file: schema.File
			if file.blob_raw_size is not None:
				raw_size_sum += file.blob_raw_size
			if file.blob_stored_size is not None:
				stored_size_sum += file.blob_stored_size
		return BackupInfo(
			id=backup.id,
			timestamp_ns=backup.timestamp,
			author=Operator.of(backup.author),
			comment=backup.comment,
			raw_size=raw_size_sum,
			stored_size=stored_size_sum,
			hidden=backup.hidden,
		)
