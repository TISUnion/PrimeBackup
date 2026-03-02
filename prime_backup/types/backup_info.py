import dataclasses
import datetime
import functools
import json
from typing import List, TYPE_CHECKING, Optional

from typing_extensions import Self

from prime_backup.db import schema
from prime_backup.types.backup_tags import BackupTags
from prime_backup.types.operator import Operator
from prime_backup.types.timestamp import Timestamp
from prime_backup.utils import conversion_utils

if TYPE_CHECKING:
	from prime_backup.types.file_info import FileInfo


# https://stackoverflow.com/questions/76656973/using-a-cached-property-on-a-named-tuple
@dataclasses.dataclass(frozen=True)
class BackupInfo:
	id: int
	timestamp: Timestamp
	creator: Operator
	comment: str
	targets: List[str]
	tags: BackupTags

	fileset_id_base: int
	fileset_id_delta: int

	file_count: int
	raw_size: int  # uncompressed size
	stored_size: int  # actual size

	files: List['FileInfo']

	@functools.cached_property
	def date(self) -> datetime.datetime:
		return self.timestamp.to_local_date()

	@functools.cached_property
	def date_str(self) -> str:
		return conversion_utils.datetime_to_str(self.timestamp.to_local_date())

	@classmethod
	def of(cls, backup: schema.Backup, *, backup_files: Optional[List[schema.File]] = None) -> 'Self':
		"""
		Notes: should be inside a session
		"""
		from prime_backup.types.file_info import FileInfo
		return cls(
			id=backup.id,
			timestamp=Timestamp.from_second_and_nano(backup.timestamp, backup.timestamp_ns_part),
			creator=Operator.of(backup.creator),
			comment=backup.comment,
			targets=list(backup.targets),
			tags=BackupTags(backup.tags),
			fileset_id_base=backup.fileset_id_base,
			fileset_id_delta=backup.fileset_id_delta,
			file_count=backup.file_count or 0,
			raw_size=backup.file_raw_size_sum or 0,
			stored_size=backup.file_stored_size_sum or 0,
			files=[FileInfo.of(file) for file in backup_files] if backup_files is not None else [],
		)

	def create_meta_buf(self) -> bytes:
		from prime_backup.types.backup_meta import BackupMeta
		meta = BackupMeta.from_backup(self)
		return json.dumps(meta.to_dict(), indent=2, ensure_ascii=False).encode('utf8')
