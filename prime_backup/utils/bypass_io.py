import io
from typing import Union, TYPE_CHECKING, Optional

if TYPE_CHECKING:
	from prime_backup.types.hash_method import HashMethod


class BypassReader(io.BytesIO):
	def __init__(self, file_obj, calc_hash: bool, *, hash_method: Optional['HashMethod'] = None):
		super().__init__()
		self.file_obj: io.BytesIO = file_obj
		self.read_len = 0

		if calc_hash:
			from prime_backup.utils import hash_utils
			self.hasher = hash_utils.create_hasher(hash_method=hash_method)
		else:
			self.hasher = None

	def read(self, *args, **kwargs):
		data = self.file_obj.read(*args, **kwargs)
		self.read_len += len(data)
		if self.hasher is not None:
			self.hasher.update(data)
		return data

	def readall(self):
		raise NotImplementedError()

	def readinto(self, b: Union[bytearray, memoryview]):
		n = self.file_obj.readinto(b)
		if n:
			self.read_len += n
			if self.hasher is not None:
				self.hasher.update(b[:n])
		return n

	def get_read_len(self) -> int:
		return self.read_len

	def get_hash(self) -> str:
		return self.hasher.hexdigest() if self.hasher is not None else ''

	def __getattribute__(self, item: str):
		if item in (
				'read', 'readall', 'readinto',
				'get_hash', 'get_read_len', 'file_obj', 'hasher', 'read_len',
		):
			return object.__getattribute__(self, item)
		else:
			return self.file_obj.__getattribute__(item)


class BypassWriter(io.BytesIO):
	def __init__(self, file_obj):
		super().__init__()
		self.file_obj: io.BytesIO = file_obj
		self.write_len = 0

	def write(self, __buffer):
		n = self.file_obj.write(__buffer)
		self.write_len += n
		return n

	def get_write_len(self) -> int:
		return self.write_len

	def __getattribute__(self, item: str):
		if item in ('write', 'get_write_len', 'file_obj', 'write_len'):
			return object.__getattribute__(self, item)
		else:
			return self.file_obj.__getattribute__(item)
