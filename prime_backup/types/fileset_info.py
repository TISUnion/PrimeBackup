import dataclasses
from typing import List, Optional

from typing_extensions import Self

from prime_backup.db import schema


@dataclasses.dataclass
class FilesetInfo:
	id: int
	is_base: bool
	base_id: int
	file_object_count: int

	file_count: int
	raw_size: int  # uncompressed size
	stored_size: int  # actual size

	# optional stats
	backup_count: int
	sampled_backup_ids: List[int]

	@classmethod
	def of(cls, file_set: schema.Fileset, *, backup_count: int = 0, sampled_backup_ids: Optional[List[int]] = None) -> Self:
		return cls(
			id=file_set.id,
			base_id=file_set.base_id,
			is_base=file_set.base_id == 0,
			file_object_count=file_set.file_object_count,
			file_count=file_set.file_count,
			raw_size=file_set.file_raw_size_sum,
			stored_size=file_set.file_stored_size_sum,
			backup_count=backup_count,
			sampled_backup_ids=sampled_backup_ids or [],
		)
