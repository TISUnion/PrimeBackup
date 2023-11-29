from dataclasses import dataclass
from typing import Optional

from prime_backup.types.operator import Operator


@dataclass
class BackupFilter:
	author: Optional[Operator] = None
	timestamp_start: Optional[int] = None
	timestamp_end: Optional[int] = None
	hidden: Optional[bool] = None
