import dataclasses
import enum
import importlib
from typing import Protocol


class Hasher(Protocol):
	def update(self, b: bytes):
		...

	def hexdigest(self) -> str:
		...


@dataclasses.dataclass(frozen=True)
class _HashMethodItem:
	hasher_func: str
	hex_length: int

	def create_hasher(self) -> Hasher:
		mod_name, func_name = self.hasher_func.split('.')
		mod = importlib.import_module(mod_name)
		func = getattr(mod, func_name)
		return func()


class HashMethod(enum.Enum):
	xxh128 = _HashMethodItem('xxhash.xxh128', 32)
	sha256 = _HashMethodItem('hashlib.sha256', 64)
	blake3 = _HashMethodItem('blake3.blake3', 64)
