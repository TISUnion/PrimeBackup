import contextlib
import os
from typing import BinaryIO, Generator

from typing_extensions import Final

from prime_backup.utils import pack_utils
from prime_backup.utils.io_types import SupportsReadAndSeek


class PackEntryReader:
	def __init__(self, file_obj: BinaryIO, offset: int, length: int):
		self.__file_obj: Final[BinaryIO] = file_obj
		self.__offset: Final[int] = offset
		self.__length: Final[int] = length
		self.__position = 0
		self.__file_obj.seek(offset)

	def read(self, size: int = -1) -> bytes:
		remaining = self.__length - self.__position
		if remaining <= 0:
			return b''
		if size < 0 or size > remaining:
			size = remaining
		data = self.__file_obj.read(size)
		self.__position += len(data)
		return data

	def seekable(self) -> bool:
		return True

	def seek(self, offset: int, whence: int = 0):
		if whence == os.SEEK_SET:
			position = offset
		elif whence == os.SEEK_CUR:
			position = self.__position + offset
		elif whence == os.SEEK_END:
			position = self.__length + offset
		else:
			raise ValueError('invalid whence {}'.format(whence))

		if position < 0:
			raise ValueError('negative seek position {}'.format(position))
		if position > self.__length:
			position = self.__length

		self.__file_obj.seek(self.__offset + position)
		self.__position = position
		return self.__position


class PackReader:
	@classmethod
	@contextlib.contextmanager
	def open_entry(cls, pack_id: int, offset: int, length: int) -> Generator[SupportsReadAndSeek, None, None]:
		pack_path = pack_utils.get_pack_path(pack_id)
		with open(pack_path, 'rb') as fh:
			yield PackEntryReader(fh, offset, length)
