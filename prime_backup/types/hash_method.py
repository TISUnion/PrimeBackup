import dataclasses
import enum
import functools
import importlib
from typing import Protocol, Union, TYPE_CHECKING, Any

HashableBuffer = Union[bytes, bytearray, memoryview]


class Hasher(Protocol):
	def update(self, buf: HashableBuffer):
		...

	def hexdigest(self) -> str:
		...


class HasherCreator(Protocol):
	def __call__(self, buf: HashableBuffer = b'') -> Hasher:
		...


@dataclasses.dataclass(frozen=True)
class _HashMethodItem:
	hasher_func: str
	hex_length: int

	@functools.cached_property
	def __get_hasher_func(self) -> Any:
		mod_name, func_name = self.hasher_func.split('.')
		mod = importlib.import_module(mod_name)
		return getattr(mod, func_name)

	def create_hasher(self, buf: HashableBuffer = b'') -> Hasher:
		return self.__get_hasher_func(buf)

	def ensure_lib(self):
		_ = self.create_hasher()


class HashMethod(enum.Enum):
	xxh128 = _HashMethodItem('xxhash.xxh128', 32)
	md5 = _HashMethodItem('hashlib.md5', 32)
	sha256 = _HashMethodItem('hashlib.sha256', 64)
	blake3 = _HashMethodItem('blake3.blake3', 64)

	if TYPE_CHECKING:
		value: _HashMethodItem


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
