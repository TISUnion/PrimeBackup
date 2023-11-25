import io
from typing import Union


# noinspection PyAbstractClass
class ByPassReader(io.BytesIO):
	def __init__(self, file_obj, do_hash: bool):
		super().__init__()
		self.file_obj: io.BytesIO = file_obj
		from xbackup.utils import hash_utils
		self.hasher = hash_utils.create_hasher() if do_hash else None
		self.read_len = 0

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
