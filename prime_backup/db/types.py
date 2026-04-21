from typing import Optional

from sqlalchemy import BINARY
from sqlalchemy.types import TypeDecorator
from typing_extensions import override


class HashHex(TypeDecorator[str]):
	"""
	SQLAlchemy custom column type.
	- DB storage : BINARY (bytes)
	- Python side: str (hex str), e.g. 'a1b2c3...'
	"""
	impl = BINARY
	cache_ok = True

	@override
	def process_bind_param(self, value: Optional[str], dialect) -> Optional[bytes]:
		if value is None:
			return None
		if isinstance(value, str):
			return bytes.fromhex(value)
		raise TypeError(f'Unsupported type for HashHex bind param: {type(value)!r}')

	@override
	def process_result_value(self, value: Optional[bytes], dialect) -> Optional[str]:
		if value is None:
			return None
		if isinstance(value, bytes):
			return value.hex()
		raise TypeError(f'Unsupported type for HashHex result value: {type(value)!r}')
