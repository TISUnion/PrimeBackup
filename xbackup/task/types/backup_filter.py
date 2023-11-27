from dataclasses import dataclass
from typing import Optional

from xbackup.task.types.operator import Operator


@dataclass
class BackupFilter:
	author: Optional[Operator] = None
	timestamp_lower: Optional[int] = None
	timestamp_upper: Optional[int] = None
