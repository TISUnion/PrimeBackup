import collections
import contextlib
import os
import threading
from typing import BinaryIO, Generator, Optional, List

from typing_extensions import Final

from prime_backup.utils import pack_utils
from prime_backup.utils.io_types import SupportsReadAndSeek


class PackFileObjectPool:
	def __init__(self, max_size: int = 4):
		if max_size <= 0:
			raise ValueError('max_size should be positive, got {}'.format(max_size))
		self.__max_size = max_size
		self.__files: 'collections.OrderedDict[int, BinaryIO]' = collections.OrderedDict()
		self.__lock = threading.Lock()
		self.__closed = False

	def __enter__(self) -> 'PackFileObjectPool':
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.close()

	@classmethod
	def __close_files(cls, files: List[BinaryIO]):
		errors: List[Exception] = []
		for file in files:
			try:
				file.close()
			except Exception as e:
				errors.append(e)
		if len(errors) > 0:
			raise Exception(f'Failed to close {len(errors)} files: {errors}')

	@contextlib.contextmanager
	def acquire(self, pack_id: int) -> Generator[BinaryIO, None, None]:
		with self.__lock:
			if self.__closed:
				raise RuntimeError('file object pool is closed')
			existing_file: Optional[BinaryIO] = self.__files.pop(pack_id, None)

		file: BinaryIO
		if existing_file is None:
			file = open(pack_utils.get_pack_path(pack_id), 'rb')
		else:
			file = existing_file

		try:
			yield file
		finally:
			to_close_files: List[BinaryIO] = []
			with self.__lock:
				if self.__closed:
					to_close_files.append(file)
				else:
					old_file = self.__files.pop(pack_id, None)
					self.__files[pack_id] = file
					self.__files.move_to_end(pack_id)

					if old_file is not None and old_file is not file:
						to_close_files.append(old_file)
					while len(self.__files) > self.__max_size:
						_, old_file = self.__files.popitem(last=False)
						to_close_files.append(old_file)
			self.__close_files(to_close_files)

	def close(self):
		with self.__lock:
			self.__closed = True
			files = list(self.__files.values())
			self.__files.clear()
		self.__close_files(files)


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
	def open_entry(cls, pack_id: int, offset: int, length: int, *, file_obj_pool: Optional[PackFileObjectPool] = None) -> Generator[SupportsReadAndSeek, None, None]:
		with contextlib.ExitStack() as es:
			file: BinaryIO
			if file_obj_pool is None:
				pack_path = pack_utils.get_pack_path(pack_id)
				file = es.enter_context(open(pack_path, 'rb'))
			else:
				file = es.enter_context(file_obj_pool.acquire(pack_id))
			yield PackEntryReader(file, offset, length)
