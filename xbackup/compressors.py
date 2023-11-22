import abc
import enum
import gzip
import io
import lzma
import shutil
from typing import BinaryIO, Union

import lz4.frame
import pyzstd
import snappy
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
	def create(cls, method: Union[str, 'CompressMethod']):
		if not isinstance(method, CompressMethod):
			if method in CompressMethod:
				method = CompressMethod[method].value
			else:
				raise ValueError(f'Unknown compression method: {method}')
		return method()

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

	def _copy_compressed(self, f_in: BinaryIO, f_out: BinaryIO):
		raise NotImplementedError()

	def _copy_decompressed(self, f_in: BinaryIO, f_out: BinaryIO):
		raise NotImplementedError()


class PlainCompressor(Compressor):
	def _copy_compressed(self, f_in: BinaryIO, f_out: BinaryIO):
		shutil.copyfileobj(f_in, f_out)

	def _copy_decompressed(self, f_in: BinaryIO, f_out: BinaryIO):
		shutil.copyfileobj(f_in, f_out)


class GzipCompressor(Compressor):
	def _copy_compressed(self, f_in: BinaryIO, f_out: BinaryIO):
		with gzip.open(f_out, 'wb') as gz_out:
			shutil.copyfileobj(f_in, gz_out)

	def _copy_decompressed(self, f_in: BinaryIO, f_out: BinaryIO):
		with gzip.open(f_in, 'rb') as gz_in:
			shutil.copyfileobj(gz_in, f_out)


class LzmaCompressor(Compressor):
	def _copy_compressed(self, f_in: BinaryIO, f_out: BinaryIO):
		with lzma.open(f_out, 'wb') as lzma_out:
			shutil.copyfileobj(f_in, lzma_out)

	def _copy_decompressed(self, f_in: BinaryIO, f_out: BinaryIO):
		with lzma.open(f_in, 'rb') as lzma_in:
			shutil.copyfileobj(lzma_in, f_out)


class ZstdCompressor(Compressor):
	def _copy_compressed(self, f_in: BinaryIO, f_out: BinaryIO):
		pyzstd.compress_stream(f_in, f_out)

	def _copy_decompressed(self, f_in: BinaryIO, f_out: BinaryIO):
		pyzstd.decompress_stream(f_in, f_out)


class SnappyCompressor(Compressor):
	def _copy_compressed(self, f_in: BinaryIO, f_out: BinaryIO):
		snappy.stream_compress(f_in, f_out)

	def _copy_decompressed(self, f_in: BinaryIO, f_out: BinaryIO):
		snappy.stream_decompress(f_in, f_out)


class Lz4Compressor(Compressor):
	def _copy_compressed(self, f_in: BinaryIO, f_out: BinaryIO):
		with lz4.frame.open(f_out, 'wb') as lz4_out:
			shutil.copyfileobj(f_in, lz4_out)

	def _copy_decompressed(self, f_in: BinaryIO, f_out: BinaryIO):
		with lz4.frame.open(f_in, 'rb') as lz4_in:
			shutil.copyfileobj(lz4_in, f_out)


class CompressMethod(enum.Enum):
	plain = PlainCompressor
	gzip = GzipCompressor
	lzma = LzmaCompressor
	zstd = ZstdCompressor
	snappy = SnappyCompressor
	lz4 = Lz4Compressor
