from dataclasses import dataclass
from typing import Optional

from prime_backup.types.operator import Operator


@dataclass
class BackupFilter:
	id_start: Optional[int] = None
	id_end: Optional[int] = None
	author: Optional[Operator] = None
	timestamp_start: Optional[int] = None
	timestamp_end: Optional[int] = None
	hidden: Optional[bool] = None
