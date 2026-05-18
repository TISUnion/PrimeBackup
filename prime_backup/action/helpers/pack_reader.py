import contextlib
import io
from pathlib import Path
from types import TracebackType
from typing import Any, BinaryIO, Generator, Optional, Type

from prime_backup.utils import pack_utils


class PackEntryReader(io.RawIOBase):
	def __init__(self, file_obj: BinaryIO, offset: int, length: int):
		self.__file_obj: Any = file_obj
		self.__offset = offset
		self.__length = length
		self.__position = 0
		self.__file_obj.seek(offset)

	def readable(self) -> bool:
		return True

	def seekable(self) -> bool:
		return self.__file_obj.seekable()

	def tell(self) -> int:
		return self.__position

	def seek(self, offset: int, whence: int = 0) -> int:
		if whence == 0:
			new_position = offset
		elif whence == 1:
			new_position = self.__position + offset
		elif whence == 2:
			new_position = self.__length + offset
		else:
			raise ValueError('invalid whence {}'.format(whence))
		if new_position < 0:
			raise ValueError('negative seek position {}'.format(new_position))
		if new_position > self.__length:
			new_position = self.__length
		self.__file_obj.seek(self.__offset + new_position)
		self.__position = new_position
		return self.__position

	def read(self, size: int = -1) -> bytes:
		remaining = self.__length - self.__position
		if remaining <= 0:
			return b''
		if size is None or size < 0 or size > remaining:
			size = remaining
		data = self.__file_obj.read(size)
		self.__position += len(data)
		return data

	def readinto(self, b: Any) -> int:
		remaining = self.__length - self.__position
		if remaining <= 0:
			return 0
		buffer = memoryview(b)
		if len(buffer) > remaining:
			buffer = buffer[:remaining]
		n = self.__file_obj.readinto(buffer)
		if n is None:
			return 0
		self.__position += n
		return n

	def close(self):
		self.__file_obj.close()
		super().close()

	def __enter__(self) -> 'PackEntryReader':
		return self

	def __exit__(self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]):
		self.close()


class PackReader:
	@staticmethod
	def get_pack_path(pack_name: str) -> Path:
		return pack_utils.get_pack_path(pack_name)

	@classmethod
	@contextlib.contextmanager
	def open_entry(cls, pack_name: str, offset: int, length: int) -> Generator[PackEntryReader, None, None]:
		pack_path = cls.get_pack_path(pack_name)
		with open(pack_path, 'rb') as fh:
			yield PackEntryReader(fh, offset, length)
