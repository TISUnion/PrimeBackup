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
	md5 = _HashMethodItem('hashlib.md5', 32)
	sha256 = _HashMethodItem('hashlib.sha256', 64)
	blake3 = _HashMethodItem('blake3.blake3', 64)


def __verify_hex_length():
	for hash_method in HashMethod:
		try:
			hasher = hash_method.value.create_hasher()
		except ImportError:
			continue
		else:
			hasher.update(b'foo')
			s = hasher.hexdigest()
			if len(s) != hash_method.value.hex_length:
				raise AssertionError('{} declares hex_length={}, but the actual hex length is {}'.format(hash_method.value, hash_method.value.hex_length, len(s)))


__verify_hex_length()
