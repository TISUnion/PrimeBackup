import contextlib
from typing import BinaryIO, Generator

from prime_backup.utils import pack_utils
from prime_backup.utils.io_types import SupportsReadBytes


class PackEntryReader:
	def __init__(self, file_obj: BinaryIO, offset: int, length: int):
		self.__file_obj = file_obj
		self.__offset = offset
		self.__length = length
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


class PackReader:
	@classmethod
	@contextlib.contextmanager
	def open_entry(cls, pack_id: int, offset: int, length: int) -> Generator[SupportsReadBytes, None, None]:
		pack_path = pack_utils.get_pack_path(pack_id)
		with open(pack_path, 'rb') as fh:
			yield PackEntryReader(fh, offset, length)
