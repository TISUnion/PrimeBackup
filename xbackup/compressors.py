import abc
import contextlib
import enum
import gzip
import io
import lzma
import shutil
from typing import BinaryIO, Union, ContextManager, Any

import lz4.frame
import pyzstd
import xxhash
from typing_extensions import final

from xbackup.types import PathLike


# noinspection PyAbstractClass
class HashingReader(io.BytesIO):
	def __init__(self, file_obj):
		super().__init__()
		self.file_obj: io.BytesIO = file_obj
		self.hasher = xxhash.xxh128()

	def read(self, *args, **kwargs):
		data = self.file_obj.read(*args, **kwargs)
		self.hasher.update(data)
		return data

	def readall(self):
		raise NotImplementedError()

	def readinto(self, b: Union[bytearray, memoryview]):
		n = self.file_obj.readinto(b)
		if n:
			self.hasher.update(b[:n])
		return n

	def get_hash(self) -> str:
		return self.hasher.hexdigest()

	def __getattribute__(self, item: str):
		if item in (
				'read', 'readall', 'readinto',
				'get_hash', 'file_obj', 'hasher',
		):
			return object.__getattribute__(self, item)
		else:
			return self.file_obj.__getattribute__(item)


class Compressor(abc.ABC):
	@classmethod
	def create(cls, method: Union[str, 'CompressMethod']) -> 'Compressor':
		if not isinstance(method, CompressMethod):
			if method in CompressMethod.__members__:
				method = CompressMethod[method]
			else:
				raise ValueError(f'Unknown compression method: {method}')
		return method.value()

	@classmethod
	def name(cls) -> str:
		return CompressMethod(cls).name

	@final
	def compress(self, source_path: PathLike, dest_path: PathLike):
		with open(source_path, 'rb') as f_in, open(dest_path, 'wb') as f_out:
			reader = HashingReader(f_in)
			self._copy_compressed(reader, f_out)
			return reader.get_hash()

	@final
	def decompress(self, source_path: PathLike, dest_path: PathLike):
		with open(source_path, 'rb') as f_in, open(dest_path, 'wb') as f_out:
			self._copy_decompressed(f_in, f_out)

	@contextlib.contextmanager
	def open_decompressed(self, source_path: PathLike) -> ContextManager[BinaryIO]:
		with open(source_path, 'rb') as f:
			with self.decompress_stream(f) as f_decompressed:
				yield f_decompressed

	@contextlib.contextmanager
	def compress_stream(self, f_out: BinaryIO) -> ContextManager[BinaryIO]:
		raise NotImplementedError()

	@contextlib.contextmanager
	def decompress_stream(self, f_in: BinaryIO) -> ContextManager[BinaryIO]:
		raise NotImplementedError()

	def _copy_compressed(self, f_in: BinaryIO, f_out: BinaryIO):
		raise NotImplementedError()

	def _copy_decompressed(self, f_in: BinaryIO, f_out: BinaryIO):
		raise NotImplementedError()


class PlainCompressor(Compressor):
	def _copy_compressed(self, f_in: BinaryIO, f_out: BinaryIO):
		shutil.copyfileobj(f_in, f_out)

	def _copy_decompressed(self, f_in: BinaryIO, f_out: BinaryIO):
		shutil.copyfileobj(f_in, f_out)

	@contextlib.contextmanager
	def compress_stream(self, f_out: BinaryIO) -> ContextManager[BinaryIO]:
		yield f_out

	@contextlib.contextmanager
	def decompress_stream(self, f_in: BinaryIO) -> ContextManager[BinaryIO]:
		yield f_in


class _GzipLikeCompressorBase(Compressor):
	_lib: Any

	def _copy_compressed(self, f_in: BinaryIO, f_out: BinaryIO):
		with self._lib.open(f_out, 'wb') as compressed_out:
			shutil.copyfileobj(f_in, compressed_out)

	def _copy_decompressed(self, f_in: BinaryIO, f_out: BinaryIO):
		with self.decompress_stream(f_in) as compressed_in:
			shutil.copyfileobj(compressed_in, f_out)

	@contextlib.contextmanager
	def compress_stream(self, f_out: BinaryIO) -> ContextManager[BinaryIO]:
		with self._lib.open(f_out, 'wb') as compressed_out:
			yield compressed_out

	@contextlib.contextmanager
	def decompress_stream(self, f_in: BinaryIO) -> ContextManager[BinaryIO]:
		with self._lib.open(f_in, 'rb') as compressed_in:
			yield compressed_in


class GzipCompressor(_GzipLikeCompressorBase):
	_lib = gzip


class LzmaCompressor(_GzipLikeCompressorBase):
	_lib = lzma


class ZstdCompressor(_GzipLikeCompressorBase):
	_lib = pyzstd


class Lz4Compressor(_GzipLikeCompressorBase):
	_lib = lz4.frame


class CompressMethod(enum.Enum):
	plain = PlainCompressor
	gzip = GzipCompressor
	lzma = LzmaCompressor
	zstd = ZstdCompressor
	lz4 = Lz4Compressor
