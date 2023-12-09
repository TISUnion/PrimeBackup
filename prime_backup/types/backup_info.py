import dataclasses
import datetime
import functools

from prime_backup.db import schema
from prime_backup.types.backup_tags import BackupTags
from prime_backup.types.operator import Operator
from prime_backup.utils import conversion_utils


# https://stackoverflow.com/questions/76656973/using-a-cached-property-on-a-named-tuple
@dataclasses.dataclass(frozen=True)
class BackupInfo:
	id: int
	timestamp_ns: int
	author: Operator
	comment: str
	tags: BackupTags

	raw_size: int  # uncompressed size
	stored_size: int  # actual size

	@functools.cached_property
	def date(self) -> datetime.datetime:
		return conversion_utils.timestamp_to_local_date(self.timestamp_ns)

	@functools.cached_property
	def date_str(self) -> str:
		return conversion_utils.timestamp_to_local_date_str(self.timestamp_ns)

	@classmethod
	def of(cls, backup: schema.Backup) -> 'BackupInfo':
		"""
		Notes: should be inside a session
		"""
		return BackupInfo(
			id=backup.id,
			timestamp_ns=backup.timestamp,
			author=Operator.of(backup.author),
			comment=backup.comment,
			tags=BackupTags(backup.tags),
			raw_size=backup.file_raw_size_sum or 0,
			stored_size=backup.file_stored_size_sum or 0,
		)
