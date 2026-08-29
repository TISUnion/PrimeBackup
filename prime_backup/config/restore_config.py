from typing import Optional, Any

from mcdreforged.api.utils import Serializable
from typing_extensions import override


class RestoreConfig(Serializable):
	destination_root: Optional[str] = None
	create_pre_restore_backup: bool = True
	countdown_sec: int = 10
	reuse_unchanged_files: bool = False

	@override
	def validate_attribute(self, attr_name: str, attr_value: Any, **kwargs):
		if attr_name == 'destination_root' and attr_value == '':
			raise ValueError('Field destination_root must not be empty')
		if attr_name == 'countdown_sec' and attr_value < 0:
			raise ValueError('Field countdown_sec must >= 0, got {!r}'.format(attr_value))
